const knex = require('./db');

const twitter = require('./twitter');

const Bottleneck = require('bottleneck');

const twilio = require('twilio');
let twilioClient = new twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);


const AWS = require('aws-sdk');
const s3 = new AWS.S3();
s3.config.update({
    accessKeyId:    process.env.AWS_ID,
    secretAccessKey:    process.env.AWS_SECRET
});

// Milliseconds for staleAccount query (twitterAccounts to be updated)
const SINCE_LAST_CHECKED = process.env.HOURS_SINCE_LAST_CHECKED * 3600000;

// Global boolean to halt execution when 15-minute rate threshold crossed to avoid API key abuse
let rate_limited_exceeded = false;
//will set remaining in 15-minute window via initial dummy API call
let window_rate;
let total_tweets_grabbed = 0;
//Global boolean to send SMS on error
let errorCaught = false;

//helper function to decrement tweetIDs that exceed JS 32-bit int range
function decrementString(str) {
    if (!str) return '1';
    // If content (leading digit after max recursion) is 1, return empty to prevent leading 0s in specifically 10000..00
    if (str === '1') return '';
    
    let lastIndex = str.length - 1;
    let lastChar = str[lastIndex];
    
    if (str[lastIndex] === '0') {
        //Use recursion to work back from final digit until a non-zero digit is found, replacing zeros with 9s
        return decrementString(str.slice(0, -1)).concat('9');
    } else {
        //when a non-zero digit is encountered, we can decrement that digit and finish
        str = str.slice(0, -1).concat(~~lastChar - 1);
    }
    
    return str;
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
        .select('states.abbreviation', 'districts.number', 'reps.candidate_name');
}
async function getStaleStateTwitterAccounts() {
    return await knex('states')
        .join('senators', 'senators.state_id', '=', 'states.id')
        .join('twitter_accounts', 'twitter_accounts.politician_id', '=', 'senators.id')
        .where('twitter_accounts.politician_type', 'Senator')
        .where('states.tweets_last_updated', null)
        .orWhere('states.tweets_last_updated', '<', new Date(new Date() - SINCE_LAST_CHECKED))
        .select(knex.ref('states.id').as('state_id'))
        .select(knex.ref('twitter_accounts.id').as('account_id'))
        .select('twitter_accounts.since_id', 'twitter_accounts.handle', 'twitter_accounts.last_checked', 'twitter_accounts.tweet_count')
        .select('states.abbreviation', 'senators.candidate_name');
}

function groupByDistrict(accounts) {
    // Create object to track accounts updated by district
    let grouped = {};

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
                };
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
            });
    });
   
    return Object.values(grouped);
}
function groupByState(accounts) {
    // Create object to track accounts updated by state
    let grouped = {};

    accounts.forEach(a=> {
        // initialize object key with state_id if it doesn't exist
        if (!grouped[a.state_id]) {
            grouped[a.state_id] = {
                state:  a.abbreviation,
                state_id: a.state_id,
                updateReady: false,
                updateJSON: false,
                accounts: []
                };
        }
        // once initialized, twitter_accounts w relevant info can be added to accounts array
        grouped[a.state_id].accounts.push(
            {
                handle: a.handle,
                lastChecked: a.last_checked,
                twitter_account_id: a.account_id,
                tweet_count: a.tweet_count,
                sinceID: a.since_id || '1',
                // newSinceID: a.since_id, //set to default for future batch Update operation; will be replaced if new tweet found
                newTweets: [],
                checked: false
            });
    });
   
    return Object.values(grouped);
}

function fetchTweets(region) {
    return new Promise(async resolve=>{
        let promises = region.accounts.map(retrieveNewTweets);
        await Promise.all(promises);
            // .catch(()=>console.log("Rate limit exceeded"))


        //set a flag in the district/state object if every twitter account was succesfully checked AND at least one of them had new tweets
        //ONLY checking if any had new tweets could cause problems when our program exits during an API window limit
        //better to wait until the next batch run to be safe
        if (region.accounts.every(a=>a.checked || a.deleteMe)) {
            region.updateReady = true;
            if (region.accounts.some(a=>a.newTweets.length)) {
                region.updateJSON = true;
            }
        }
        resolve();
    });
}

async function retrieveNewTweets(account) {
    return new Promise(async resolve => {
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
                since_id: oldestInDB,
                exclude_replies: true,
                include_rts:    true
            };
            //maxID is only sent to API on calls after the first, when more than one batch of tweets needs to be retrieved  (and needs to be decremented as max_id is inclusive for twitter API)
            if (maxID) params.max_id = decrementString(maxID);
            
            window_rate--;
            if (window_rate < 0) rate_limited_exceeded = true;

            if (rate_limited_exceeded) {
                resolve();
                return; 
            }

            try {
                tweets = await twitter.get('statuses/user_timeline', params);
            } catch(e) {
                if (e.errors) {   // e.errors represents an error after a successful Twitter API call
                    console.log(`Error caught for ${account.handle}: ${e.errors[0].message}`);
                    if (e.errors[0].code === 34) account.deleteMe = true;
                    resolve();
                    return;
                } else {
                    let status = e._headers && e._headers.get('status');
                    errorCaught = true;
                    console.log(`Other Error caught for ${account.handle}: ${status}`);
                    if (status === '401 Unauthorized') {
                        account.deleteMe = true;
                        account.checked = true;
                        resolve();
                        return;
                    }
                }
            }

            if (tweets.length) {
                maxID = tweets[tweets.length - 1].id_str;    //save for a repeat loop now
                if (!account.newSinceID) account.newSinceID = tweets[0].id_str; //set new sinceID for DB

                let newTweetInfo = tweets.map(a=>({snowflake_id: a.id_str, created: a.created_at}));
                account.newTweets.push(...newTweetInfo);

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
        resolve();
    });
}

async function saveToDB(districts, states) {
    const tweetRows = [], districtRows = [], accountRows = [], accountDeletes = [], stateRows = [];
    for (let district of districts) {
        if (!district.updateReady) continue; // Skip if twitter update failed

        districtRows.push({id: district.district_id, tweets_last_updated: new Date()});
        for (let account of district.accounts) {
            if (account.deleteMe) accountDeletes.push(account.twitter_account_id);
            if (account.newSinceID && account.newLastChecked) { //Skip if Account invalid
                accountRows.push({id: account.twitter_account_id, since_id: account.newSinceID, last_checked: account.newLastChecked, tweet_count: account.numNew + account.tweet_count});
                for (let tweet of account.newTweets) {
                    tweetRows.push({twitter_account_id: account.twitter_account_id, ...tweet});
                }
            }    
        }
    }

    for (let state of states) {
        if (!state.updateReady) continue; // Skip if twitter update failed

        stateRows.push({id: state.state_id, tweets_last_updated: new Date()});
        for (let account of state.accounts) {
            if (account.deleteMe) accountDeletes.push(account.twitter_account_id);
            if (account.newSinceID && account.newLastChecked) { //Skip if Account invalid
                accountRows.push({id: account.twitter_account_id, since_id: account.newSinceID, last_checked: account.newLastChecked, tweet_count: account.numNew + account.tweet_count});
                for (let tweet of account.newTweets) {
                    tweetRows.push({twitter_account_id: account.twitter_account_id, ...tweet});
                }
            }    
        }
    }

    //INSERT into tweets
    console.time('insertTweets');
    await knex.batchInsert('tweets', tweetRows);
    console.timeEnd('insertTweets');
    
    //UPDATE accounts       TODO: add error handling
    console.time('updateAccounts');
    
    try {
        await knex.batchInsert('account_updates', accountRows);
        await knex.raw('UPDATE twitter_accounts SET since_id = x.since_id, last_checked = x.last_checked, tweet_count = x.tweet_count FROM account_updates x WHERE twitter_accounts.id = x.id');
        await knex('account_updates').truncate();
    } catch (e) {
        console.error(e);
    }

    console.timeEnd('updateAccounts');

    //UPDATE districts
    console.time('updateDistricts');
    
    try {
        await knex.batchInsert('district_updates', districtRows);
        await knex.raw('UPDATE districts SET tweets_last_updated = x.tweets_last_updated FROM district_updates x WHERE districts.id = x.id');
        await knex('district_updates').truncate();
    } catch (e) {
        console.error(e);
    }

    console.timeEnd('updateDistricts');

    //UPDATE states
    console.time('updateStates');
    
    try {
        await knex.batchInsert('state_updates', stateRows);
        await knex.raw('UPDATE states SET tweets_last_updated = x.tweets_last_updated FROM state_updates x WHERE states.id = x.id');
        await knex('state_updates').truncate();
    } catch (e) {
        console.error(e);
    }

    console.timeEnd('updateStates');

    if (accountDeletes.length) {
        //DELETE deleted / privatized twitter_accounts & any associated tweets
        console.time('deleteAccounts');
        await knex.transaction(async trx=> {
            return Promise.all(accountDeletes.map(account=> knex('twitter_accounts').where('id', account).del().transacting(trx)));
        });
        await knex.transaction(async trx=> {
            return Promise.all(accountDeletes.map(account=> knex('tweets').where('twitter_account_id', account).del().transacting(trx)));
        });
        console.timeEnd('deleteAccounts');
    }
    return 'pizza';
}

async function getRateLimit() {
    let response = await twitter.get('statuses/user_timeline', {screen_name: 'realdonaldtrump', count: 1});

    window_rate = ~~response._headers.get('x-rate-limit-remaining');
    let reset_time = (new Date(response._headers.get('x-rate-limit-reset') * 1000)).toString();
    console.log(`Calls remaining this window: ${window_rate}`);
    console.log(`Resets at: ${reset_time}`);
}

async function uploadToS3(JSONobject, filename) {
    const params = {
        Bucket: process.env.AWS_BUCKET,
        Key: filename,
        Body:   JSON.stringify(JSONobject),
        ContentType:    "application/json",
        ACL:    'public-read',
        CacheControl: `max-age=${3600 * process.env.HOURS_SINCE_LAST_CHECKED}`
    };

    await s3.putObject(params).promise()
        .catch(err=>console.log(err));
}

async function buildJSONByDistrict(district_id) {
    let district = {};
    district.reps = await knex('reps')
        .where('reps.district_id', district_id);
        // .select('reps.candidate_loan_repayments', 'reps.candidate_name', 'reps.cash_on_hand_end', 'reps.cash_on_hand_start', 'reps.comm_refunds', 'reps.contrib_from_other_comms')
        // .select('reps.contrib_from_party_comms', 'reps.contributions_from_candidate', 'reps.coverage_end_date', 'reps.debts')
    let repIDs = district.reps.map(r=>r.id);
    let twitterAccounts = await knex('twitter_accounts')
        .where('politician_type', 'Rep')
        .whereIn('politician_id', repIDs);
    for (let account of twitterAccounts) {
        account.tweets = await knex('tweets')
            .where('twitter_account_id', account.id)
            .orderBy('created', 'desc')
            .select('snowflake_id', 'created');
        let belongsToThisRep = district.reps.find(r=>r.id == account.politician_id);
        if (!belongsToThisRep.tweets) belongsToThisRep.tweets = [];
        belongsToThisRep.tweets.push(...account.tweets);
    }
    for (let rep of district.reps) {
        if (!rep.tweets) continue; 
            rep.tweets.sort((a, b) => {
            let [one, two] = [a.snowflake_id, b.snowflake_id];
            if (one.length !== two.length) {
                return (one.length > two.length ? -1 : 1);
            }
            return (one > two ? -1 : 1);
        });
    }
    return district;
}
async function buildJSONByState(state_id) {
    let state = {};
    state.senators = await knex('senators')
        .where('senators.state_id', state_id);
    let senatorIDs = state.senators.map(s=>s.id);
    let twitterAccounts = await knex('twitter_accounts')
        .where('politician_type', 'Senator')
        .whereIn('politician_id', senatorIDs);
    for (let account of twitterAccounts) {
        account.tweets = await knex('tweets')
            .where('twitter_account_id', account.id)
            .orderBy('created', 'desc')
            .select('snowflake_id', 'created');
        let belongsToThisSenator = state.senators.find(s=>s.id == account.politician_id);
        if (!belongsToThisSenator.tweets) belongsToThisSenator.tweets = [];
        belongsToThisSenator.tweets.push(...account.tweets);
    }
    for (let senator of state.senators) {
        if (!senator.tweets) continue;
        senator.tweets.sort((a, b) => {
            let [one, two] = [a.snowflake_id, b.snowflake_id];
            if (one.length !== two.length) {
                return (one.length > two.length ? -1 : 1);
            }
            return (one > two ? -1 : 1);
        });
    }
    return state;
}

exports.handler = async () => {
    const timeStart = new Date();
    console.time('twitter');
    
    let response = await getStaleDistrictTwitterAccounts();
    let districts = groupByDistrict(response);
    
    response = await getStaleStateTwitterAccounts();
    let states = groupByState(response);

    await getRateLimit();
    
    const limiter = new Bottleneck({
        maxConcurrent: process.env.MAX_CONCURRENT_FETCHES
    });

    const throttledFetchTweets = limiter.wrap(fetchTweets);

    //Could probably combine into one routine but maybe good to do districts first to perhaps complete in case both cannot get in under API cap
    let promises = districts.map(throttledFetchTweets);
    await Promise.all(promises);
    
    promises = states.map(throttledFetchTweets);
    await Promise.all(promises);
    
    console.timeEnd('twitter');
    console.log(`Total Tweets Grabbed: ${total_tweets_grabbed}`);
    console.log(`Calls remaining this window: ${window_rate}`);
    console.time('db push');

    await saveToDB(districts, states);
    console.timeEnd('db push');
    
    const staleJSON = districts.filter(d=>d.updateJSON);   //rebuild JSON file if new tweets were acquired
    const numDistricts = staleJSON.length;
    staleJSON.push(...states.filter(s=>s.updateJSON));

    const JSONSUpdated = staleJSON.length;
    console.log(`${JSONSUpdated} districts/states need new JSON files prepared`);
    console.time('total json creation');

    promises = [];

    for (let region of staleJSON) {
        let filename, data;
        if (region.district_id) {
            filename = `${region.state}-${region.district}.json`;
            data = await buildJSONByDistrict(region.district_id);
        } else {
            filename = `${region.state}.json`;
            data = await buildJSONByState(region.state_id);
        }

        promises.push(uploadToS3(data, filename));

    }
    await Promise.all(promises);
    console.timeEnd('total json creation');
    const timeElapsed = Math.floor((new Date() - timeStart) / 1000);

    const message = `Run complete\n----------------\nTweets Grabbed: ${total_tweets_grabbed}\nJSON Updated: ${JSONSUpdated}\n--States: ${JSONSUpdated - numDistricts}\n--Districts: ${numDistricts}\n----------------\nTime to Run: ${timeElapsed} seconds`;
    if (process.env.SEND_SUMMARY_SMS) {
        try {
            await twilioClient.messages.create({
                body: message,
                to: process.env.TWILIO_ADMIN_MOBILE,
                from: process.env.TWILIO_FROM
            });
        } catch(e) {
            console.log(e);
        }
    }
    
    if (process.env.SEND_SMS_ON_ERROR && errorCaught) {
        try {
            await twilioClient.messages.create({
                body: 'Error detected in .',
                to: process.env.TWILIO_ADMIN_MOBILE,
                from: process.env.TWILIO_FROM
            });
        } catch(e) {
            console.log(e);
        }
    }
    await knex.destroy();
    return {
        result: message
    };
};
