require('dotenv').config();
let Twitter = require('twitter-lite');

let client = new Twitter({
    consumer_key:       process.env.TWITTER_CONSUMER_KEY,
    consumer_secret:    process.env.TWITTER_CONSUMER_SECRET,
    bearer_token:       process.env.TWITTER_BEARER_TOKEN
});

module.exports = client