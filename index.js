const knex = require('./db')

const twitter = require('./twitter');

const twilio = require('twilio')
let twilioClient = new twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);

const AWS = require('aws-sdk')
const s3 = new AWS.S3();
s3.config.update({
    accessKeyId:    process.env.AWS_ID,
    secretAccessKey:    process.env.AWS_SECRET
});

const cliProgress = require('cli-progress');
let queryProgress, s3UploadsProgress;

// Milliseconds for staleAccount query (twitterAccounts to be updated)
const SINCE_LAST_CHECKED = process.env.HOURS_SINCE_LAST_CHECKED * 3600000

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
                // newSinceID: a.since_id, //set to default for future batch Update operation; will be replaced if new tweet found
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
        
        window_rate--;
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
                let status = e._headers && e._headers.get('status')
                console.log(`Other Error caught for ${account.handle}: ${status}`)
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
            tweetsAvailable = false;
            if (!account.newSinceID) account.newSinceID = account.sinceID;
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
    console.time('insertTweets')
    result.insertTweets = await knex.batchInsert('tweets', tweetRows)
    console.timeEnd('insertTweets')
    
    //UPDATE accounts       TODO: add error handling
    console.time('updateAccounts')
    
    try {
        await knex.batchInsert('account_updates', accountRows);
        await knex.raw('UPDATE twitter_accounts SET since_id = x.since_id, last_checked = x.last_checked, tweet_count = x.tweet_count FROM account_updates x WHERE twitter_accounts.id = x.id');
        await knex('account_updates').truncate();
    } catch (e) {
        console.error(e);
    }

    console.timeEnd('updateAccounts')

    //UPDATE districts
    console.time('updateDistricts')
    
    try {
        await knex.batchInsert('district_updates', districtRows)
        await knex.raw('UPDATE districts SET tweets_last_updated = x.tweets_last_updated FROM district_updates x WHERE districts.id = x.id')
        await knex('district_updates').truncate();
    } catch (e) {
        console.error(e);
    }

    console.timeEnd('updateDistricts')

    if (accountDeletes.length) {
        //DELETE deleted / privatized twitter_accounts & any associated tweets
        console.time('deleteAccounts')
        result.deleteAccounts = await knex.transaction(async trx=> {
            return Promise.all(accountDeletes.map(account=> knex('twitter_accounts').where('id', account).del().transacting(trx)))
        })
        result.deleteTweets = await knex.transaction(async trx=> {
            return Promise.all(accountDeletes.map(account=> knex('tweets').where('twitter_account_id', account).del().transacting(trx)))
        })
        console.timeEnd('deleteAccounts')
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

async function uploadToS3(JSONobject, filename) {
    const params = {
        Bucket: process.env.AWS_BUCKET,
        Key: filename,
        Body:   JSON.stringify(JSONobject),
        ContentType:    "application/json",
        ACL:    'public-read',
        CacheControl: `max-age=${3600 * process.env.HOURS_SINCE_LAST_CHECKED}`
    }

    await s3.putObject(params).promise()
        .then(()=>s3UploadsProgress.increment())
        .catch(err=>console.log(err))
}

async function buildJSONByDistrict(district_id) {
    let district = {};
    district.reps = await knex('reps')
        .where('reps.district_id', district_id)
        // .select('reps.candidate_loan_repayments', 'reps.candidate_name', 'reps.cash_on_hand_end', 'reps.cash_on_hand_start', 'reps.comm_refunds', 'reps.contrib_from_other_comms')
        // .select('reps.contrib_from_party_comms', 'reps.contributions_from_candidate', 'reps.coverage_end_date', 'reps.debts')
    let repIDs = district.reps.map(r=>r.id)
    let twitterAccounts = await knex('twitter_accounts')
        .where('politician_type', 'Rep')
        .whereIn('politician_id', repIDs)
    for (account of twitterAccounts) {
        account.tweets = await knex('tweets')
            .where('twitter_account_id', account.id)
            .orderBy('created', 'desc')
            .select('snowflake_id', 'created')
        let belongsToThisRep = district.reps.find(r=>r.id == account.politician_id)
        if (!belongsToThisRep.twitterAccounts) belongsToThisRep.twitterAccounts = [];
        belongsToThisRep.twitterAccounts.push(account)
    }
    return district;
}

async function deleteThese(accounts) {
    return new Promise(resolve => {
        knex.transaction(async trx=> {
            await Promise.all(accounts.map(dupe=>knex('tweets').where('snowflake_id', dupe.snowflake_id).limit(dupe.count - 1).del().transacting(trx)))
            resolve();
        })
    })
}

async function deleteDuplicateTweets() {
    let dupes = await knex.raw('SELECT snowflake_id, COUNT(*) FROM tweets GROUP BY snowflake_id HAVING COUNT(*) > 1')
    console.log('here we go')

    while (dupes.rows.length) {
        let remaining = dupes.rows.length;
        console.log(`${remaining} dupes remaining`)
        console.time('delete')
        let promises = [];
        for (let i = 1; i < 16; i++) {
            promises.push(dupes.rows.splice(remaining - i, 1))
        }
        
        await Promise.all(promises.map(deleteThese))
        
        console.timeEnd('delete')
    }

}

(async () => {
    const timeStart = new Date();
    console.time('twitter')
    let accounts = await getStaleDistrictTwitterAccounts();
    console.log(accounts.length)
    let districts = groupByDistrict(accounts);

    await getRateLimit();

    let promises = districts.map(fetchTweets)
    await Promise.all(promises)

    console.timeEnd('twitter')
    console.log(`Total Tweets Grabbed: ${total_tweets_grabbed}`)
    console.log(`Calls remaining this window: ${window_rate}`)
    console.time('db push');

    await saveToDB(districts);
    console.timeEnd('db push')
    
    const staleJSON = districts.filter(d=>d.updateJSON);   //rebuild JSON file if new tweets were acquired
    const JSONSUpdated = staleJSON.length;
    console.log(`${JSONSUpdated} districts need new JSON files prepared`)
    console.time('total json creation')

    promises = [];
    const multibar = new cliProgress.MultiBar({clearOnComplete: false, hideCursor: true}, cliProgress.Presets.shades_grey)
    queryProgress = multibar.create(JSONSUpdated, 0, {format: 'JSON Build [{bar}] {percentage}% | {value}/{total} | ETA: {eta}s', stopOnComplete: true});
    s3UploadsProgress = multibar.create(JSONSUpdated, 0, {format: 'S3 Upload [{bar}] {percentage}% | {value}/{total} | ETA: {eta}s', clearOnComplete: true});

    for (let district of staleJSON) {
        let filename = `${district.state}-${district.district}.json`
        let data = await buildJSONByDistrict(district.district_id)
        queryProgress.increment();
        
        promises.push(uploadToS3(data, filename))

    }
    await Promise.all(promises)
    
    const timeElapsed = Math.floor((new Date() - timeStart) / 1000)

    const message = `Run complete\n----------------\nTweets Grabbed: ${total_tweets_grabbed}\nJSON Updated: ${JSONSUpdated}\n----------------\nTime to Run: ${timeElapsed} seconds`
    try {
        await twilioClient.messages.create({
            body: message,
            to: process.env.TWILIO_ADMIN_MOBILE,
            from: process.env.TWILIO_FROM
        })
    } catch(e) {
        console.log(e)
    }
    multibar.stop();
    knex.destroy()
})();
