const knex = require('./db')
const twitter = require('./twitter');

// Milliseconds for staleAccount query (twitterAccounts to be updated)
const SINCE_LAST_CHECKED = process.env.HOURS_SINCE_LAST_CHECKED * 360000

// Global boolean to halt execution when 15-minute rate threshold crossed to avoid API key abuse
let rate_limited_exceeded = false;
//will set remaining in 15-minute window via initial dummy API call
let window_rate;
let total_tweets_grabbed = 0;

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
        .select('twitter_accounts.since_id', 'twitter_accounts.handle', 'twitter_accounts.last_checked', 'twitter_accounts.tweet_count')
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
                district_id: a.district_id,
                updateReady: false,
                updateJSON: false,
                accounts: []
                }
        }
        // once initialized, twitter_accounts w relevant info can be added to accounts array
        grouped[a.district_id].accounts.push(
            {
                handle: a.handle,
                lastChecked: a.last_checked,
                twitter_account_id: a.account_id,
                tweet_count: a.tweet_count,
                sinceID: a.since_id || '1',
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
        if (district.accounts.every(a=>a.checked || a.deleteMe)) {
            district.updateReady = true;
            if (district.accounts.some(a=>a.newTweets.length)) {
                district.updateJSON = true;
            }
        }
        resolve();
    })
}

async function retrieveNewTweets(account) {
    //Used by Twitter for pagination
    let oldestInDB = account.sinceID;
    let maxID;
    let tweetsAvailable = true;
    let tweets = [];

    while (tweetsAvailable) {
        const params = {
            screen_name: account.handle,
            count: 200,
            trim_user: true,
            since_id: oldestInDB
        }
        //maxID is only sent to API on calls after the first, when more than one batch of tweets needs to be retrieved
        if (maxID) {params.max_id = maxID}
        
        window_rate -= 1;
        if (window_rate < 0) { rate_limited_exceeded = true;}

        if (rate_limited_exceeded) {
            return //Promise.reject('3423rasdfsf')//Anything more useful?  reject? 
        }

        try {
            tweets = await twitter.get('statuses/user_timeline', params)
        } catch(e) {
            if (e.errors) {   // e.errors represents an error after a successful Twitter API call
                console.log(`Error caught for ${account.handle}: ${e.errors[0].message}`)
                if (e.errors[0].code === 34) account.deleteMe = true;
                return //Promise.resolve?
            } else {
                let status = e._headers.get('status')
                console.log(`Other Error: ${status}`)
                if (status === '401 Unauthorized') account.deleteMe = true;
                account.checked = true;    //consider adding a retry after we ascertain which errors throws this clause
                // will future calls succeed? should we log this?
                return //Promise.resolve / reject? and switch main code to allSettled?
            }
        }

        if (tweets.length) {
            maxID = tweets[tweets.length - 1].id_str    //save for a repeat loop now
            if (!account.newSinceID) account.newSinceID = tweets[0].id_str; //set new sinceID for DB

            let newTweetInfo = tweets.map(a=>({snowflake_id: a.id_str, created: a.created_at}))
            account.newTweets.push(...newTweetInfo)

            total_tweets_grabbed += newTweetInfo.length;
            if (tweets.length < 180) tweetsAvailable = false;
        } else {
            console.log(`No New Tweets found for ${account.handle}`)
            tweetsAvailable = false;
        }    
    }
    account.checked = true;
    account.newLastChecked = new Date();
    account.numNew = account.newTweets.length;
}

async function saveToDB(districts) {
    const tweetRows = [], districtRows = [], accountRows = [], accountDeletes = [];
    for (let district of districts) {
        if (!district.updateReady) continue; // Skip if twitter update failed

        districtRows.push({id: district.district_id, tweets_last_updated: new Date()})
        for (let account of district.accounts) {
            if (account.deleteMe) accountDeletes.push(account.twitter_account_id)
            if (account.newSinceID && account.newLastChecked) { //Skip if Account invalid
                accountRows.push({id: account.twitter_account_id, since_id: account.newSinceID, last_checked: account.newLastChecked, tweet_count: account.numNew + account.tweet_count})
                for (let tweet of account.newTweets) {
                    tweetRows.push({twitter_account_id: account.twitter_account_id, ...tweet})
                }
            }    
        }
    }
    let result = {};
    //INSERT into tweets
    result.insertTweets = await knex.batchInsert('tweets', tweetRows)

    //UPDATE accounts       TODO: add error handling
    result.updateAccounts = await knex.transaction(async trx => {
        return Promise.all(accountRows.map(account=> knex('twitter_accounts').where('id', account.id).update(account).transacting(trx)))
    })

    //UPDATE districts
    result.updateDistricts = await knex.transaction(async trx=> {
        return Promise.all(districtRows.map(district=> knex('districts').where('id', district.id).update(district).transacting(trx)))
    })

    if (accountDeletes.length) {
        //DELETE deleted / privatized twitter_accounts & any associated tweets
        result.deleteAccounts = await knex.transaction(async trx=> {
            return Promise.all(accountDeletes.map(account=> knex('twitter_accounts').where('id', account).del().transacting(trx)))
        })
        result.deleteTweets = await knex.transaction(async trx=> {
            return Promise.all(accountDeletes.map(account=> knex('tweets').where('twitter_account_id', account).del().transacting(trx)))
        })
    }
    return result;
}

async function getRateLimit() {
    let response = await twitter.get('statuses/user_timeline', {screen_name: 'realdonaldtrump', count: 1})

    window_rate = ~~response._headers.get('x-rate-limit-remaining')
    let reset_time = (new Date(response._headers.get('x-rate-limit-reset') * 1000)).toString();
    console.log(`Calls remaining this window: ${window_rate}`)
    console.log(`Resets at: ${reset_time}`)
}

(async () => {
    console.time('twitter')
    let accounts = await getStaleDistrictTwitterAccounts();
    let districts = groupByDistrict(accounts);
    // districts = districts.slice(0, 25)

    await getRateLimit();

    let promises = districts.map(fetchTweets)
    await Promise.all(promises)

    console.timeEnd('twitter')
    console.log(`Total Tweets Grabbed: ${total_tweets_grabbed}`)
    console.log(`Calls remaining this window: ${window_rate}`)
    await saveToDB(districts);


    knex.destroy()
})();
