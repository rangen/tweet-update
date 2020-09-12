const knex = require('./db')
const twitter = require('./twitter');

// Milliseconds for staleAccount query (twitterAccounts to be updated)
const SINCE_LAST_CHECKED = process.env.HOURS_SINCE_LAST_CHECKED * 360000

// Global boolean to halt execution when 15-minute rate threshold crossed to avoid API key abuse
let rate_limited_exceeded = false;
//will set remaining in 15-minute window via initial dummy API call
let window_rate;

//helper function to decrement tweetIDs that exceed JS 32-bit int range
function decrementString(str) {
    if (!str) return '1'
    // If content (leading digit after max recursion) is 1, return empty to prevent leading 0s in specifically 10000..00
    if (str === '1') return ''
    
    lastIndex = str.length - 1;
    lastChar = str[lastIndex];
    
    if (str[lastIndex] === '0') {
        //Use recursion to work back from final digit until a non-zero digit is found, replacing zeros with 9s
        return decrementString(str.slice(0, -1)).concat('9')
    } else {
        //when a non-zero digit is encountered, we can decrement that digit and finish
        str = str.slice(0, -1).concat(~~lastChar - 1)
    }
    
    return str
}

async function getStaleDistrictTwitterAccounts() {
    return await knex('districts')
        .join('reps', 'reps.district_id', '=', 'districts.id')
        .join('twitter_accounts', 'twitter_accounts.politician_id', '=', 'reps.id')
        .join('states', 'states.id', '=', 'districts.state_id')
        .where('twitter_accounts.politician_type', 'Rep')
        .where('districts.tweets_last_updated', null)
        .orWhere('districts.tweets_last_updated', '<', new Date(new Date() - SINCE_LAST_CHECKED))
        .select(knex.ref('districts.id').as('district_id'))
        .select(knex.ref('twitter_accounts.id').as('account_id'))
        .select('twitter_accounts.since_id', 'twitter_accounts.max_id', 'twitter_accounts.handle', 'twitter_accounts.last_checked', 'twitter_accounts.use')
        .select('states.abbreviation', 'districts.number', 'reps.candidate_name')
}

function groupByDistrict(accounts) {
    // Create object to track accounts updated by district
    grouped = {};

    accounts.forEach(a=> {
        // initialize object key with district_id if it doesn't exist
        if (!grouped[a.district_id]) {
            grouped[a.district_id] = {
                state:  a.abbreviation,
                district:   a.number,
                updateReady: false,
                accounts: []
                }
        }
        // once initialized, twitter_accounts w relevant info can be added to accounts array
        grouped[a.district_id].accounts.push(
            {
                handle: a.handle,
                lastChecked: a.last_checked,
                sinceID: a.since_id,
                maxID: a.max_id,
                newTweets: [],
                checked: false
            })
    })
   
    return Object.values(grouped)
}

function fetchTweets(district) {
    return new Promise(async resolve=>{
        let promises = district.accounts.map(retrieveNewTweets)
        await Promise.all(promises)
            // .catch(()=>console.log("Rate limit exceeded"))


        //set a flag in the district object if every twitter account was succesfully checked AND at least one of them had new tweets
        //ONLY checking if any had new tweets could cause problems when our program exits during an API window limit
        //better to wait until the next batch run to be safe
        if (district.accounts.every(a=>a.checked || a.deleteMe) && district.accounts.some(a=>a.newTweets.length)) {
            district.updateReady = true;
        }
        resolve();
    })
}

async function retrieveNewTweets(account) {
    //Used by Twitter for pagination
    let sinceID = account.sinceID;
    let maxID;
    let tweetsAvailable = true;
    let tweets = [];

    while (tweetsAvailable) {
        const params = {
            screen_name: account.handle,
            count: 200,
            trim_user: true,
            since_id: decrementString(sinceID)
            //Intentionally decrement since_id string to 'overlap' with a tweet we already have saved in db to ensure data
            //continuity without an extra API call for a []     be sure to not re-save this to db later
        }
        //maxID is only sent to API on calls after the first, when more than one batch of tweets needs to be retrieved
        if (maxID) {params.max_id = maxID}
        
        window_rate -= 1;
        if (window_rate <= 1000) { rate_limited_exceeded = true;}

        if (rate_limited_exceeded) {
            console.log(`Rate Limit exceeded. Could not retrieve for ${account.handle}`)
            return //Promise.reject('3423rasdfsf')//Anything more useful?  reject? 
        }
        console.log(`Attempting to fetch with since_id: ${params.since_id} and max_id: ${maxID} for ${account.handle}`)
        try {
            tweets = await twitter.get('statuses/user_timeline', params)
        } catch(e) {
            if (e.errors) {
                console.log(`Error caught for ${account.handle}: ${e.errors[0].message}`)
                if (e.errors[0].code === 34) account.deleteMe = true;
            } else {
                console.log(`Other Error: ${e}`)
            }
        }
        //Twitter API returns [] tweets if none available since since_id
        //but we decremented above, so we should always get 1 tweet, unless they deleted that tweet, or their account
        //maxID is inclusive, so needs to be decremented to avoid overlap

        if (tweets.length) {
            let lastReturnedTweet = tweets.pop();
            maxID = lastReturnedTweet.id_str
            //if new tweets
            if (tweets.length) {
                let newTweetInfo = tweets.map(a=>({tweetID: a.id_str, createdAt: a.created_at}))
                account.newTweets.push(...newTweetInfo)
                account.newSinceID = account.newTweets[0].tweetID;
                console.log(`Found ${newTweetInfo.length} new tweets for ${account.handle}`)
            } else {
                console.log(`No New Tweets found for ${account.handle}`)
                tweetsAvailable = false;
            }    
                //if last tweetID returned from API matches sinceID from DB (from a previous session call), no more tweets to grab
            if (maxID === sinceID) {
                account.checked = true;
                account.newLastChecked = new Date();
                tweetsAvailable = false;
            }
        } else { 
            tweetsAvailable = false;
            account.checked = true;
            account.newlastChecked = new Date();
        }
    }
}

async function getRateLimit() {
    let response = await twitter.get('statuses/user_timeline', {screen_name: 'realdonaldtrump', count: 1})

    window_rate = ~~response._headers.get('x-rate-limit-remaining')
    console.log(`Calls remaining this window: ${window_rate}`)
}

(async () => {
    let accounts = await getStaleDistrictTwitterAccounts();
    let districts = groupByDistrict(accounts);
    districts = districts.slice(49, 50)

    await getRateLimit();

    let promises = districts.map(fetchTweets)
    await Promise.all(promises)


    console.dir(districts)

    knex.destroy()
        .then(console.log("DB conn destroyed"))
})();
