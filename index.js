const knex = require('./db')
const twitter = require('./twitter');

// Milliseconds for staleAccount query (twitterAccounts to be updated)
const SINCE_LAST_CHECKED = process.env.HOURS_SINCE_LAST_CHECKED * 360000

// Global boolean to halt execution when 15-minute rate threshold crossed to avoid API key abuse
let rate_limited_exceeded = false;

//helper function to decrement tweetIDs that exceed JS 32-bit int range
function decrementString(str) {
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
    return grouped;
}

function fetchTweets(district) {
    district.accounts.forEach(async (account)=> 
        await retrieveNewTweets(account)
    )
    return district;
}

async function retrieveNewTweets(account) {
    //Used by Twitter for pagination
    let sinceID = account.sinceID;
    let maxID;
    let tweetsAvailable = true;

    while (tweetsAvailable) {
        const config = {
            screen_name: account.handle,
            count: 200,
            trim_user: true,
            since_id: sinceID
        }

        if (maxID) {config.max_id = maxID}
    
        // try {
            console.log(`Attempting to fetch with since_id: ${sinceID} and max_id: ${maxID}`)
            let tweets = await twitter.get('statuses/user_timeline', config)
        // } catch (e) {
        //     console.dir(e.errors)
        // }
        //Twitter API returns [] tweets if none available since since_id
        //maxID is inclusive, so needs to be decremented to avoid overlap
        if (tweets && tweets.length) {
            let tweetInfo = tweets.map(a=>({tweetID: a.id_str, createdAt: a.created_at}))
            account.newTweets.push(...tweetInfo)
            maxID = decrementString(tweets[tweets.length - 1].id_str)
            console.log(`Found ${tweetInfo.length} for ${account.handle}`)
        } else {
            console.log(`No More Tweets for ${account.handle}`)
            account.newSinceID = sinceID;
            account.newLastChecked = new Date();
            tweetsAvailable = false;
        }
    }
}

// getStaleDistrictTwitterAccounts()
//     .then(accts=>groupByDistrict(accts))
//     .then(grouped=>fetchTweets(grouped))
//     .then(districts=>console.log(districts["2"]))
//     .catch(e=>console.log(e))
//     .finally(()=>knex.destroy());

getStaleDistrictTwitterAccounts()
    .then(accounts=>groupByDistrict(accounts))
    .then(districts=> {
        Promise.all(Object.keys(districts).slice(-5).map(d=>fetchTweets(districts[d])))
    })
    .catch(e=>console.log(`Error: ${e}`))