const express = require("express");
const bodyParser = require("body-parser");
const request = require("request");
const socketIo = require("socket.io");
const http = require("http");
const { Kafka } = require('kafkajs');



module.exports = {twitterStreamApi};


async function twitterStreamApi(io) 
{
    /*const app = express();

    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: true }));

    const server = http.createServer(app);
    const io = socketIo(server);*/

    const BEARER_TOKEN = process.env.TWITTER_BEARER_TOKEN;

    let timeout = 0;

    const streamURL = new URL(
    "https://api.twitter.com/2/tweets/search/stream?tweet.fields=attachments,author_id,geo,in_reply_to_user_id,reply_settings,source,entities&media.fields=preview_image_url,url&place.fields=country"
    );

    const rulesURL = new URL(
    "https://api.twitter.com/2/tweets/search/stream/rules"
    );

    const errorMessage = {
        title: "Please Wait",
        detail: "Waiting for new Tweets to be posted...",
    };

    const authMessage = {
        title: "Could not authenticate",
        details: [
            `Please make sure your bearer token is correct. 
            If using Glitch, remix this app and add it to the .env file`,
        ],
        type: "https://developer.twitter.com/en/docs/authentication",
    };

    // Kafka client setup
    const kafka = new Kafka({
        clientId: 'ghg-twitter-stream-listener',
        brokers: ['localhost:9093'],
    })


    // Kafka Producer
    const producer = kafka.producer();


    // Stream main Login
    const streamTweets = async (socket, token) => {
        let stream;

        const config = {
            url: streamURL,
            auth: {
            bearer: token,
            },
            timeout: 31000,
        };

        // Connect to Kafka producer
        try 
        {
            await producer.connect();
        
        } catch(e) 
        { 
            socket.emit("producerConnectError", e);
            last_message = {event: 'producerConnectError', message: e};
        }

        try {
            const stream = request.get(config);

            console.log('antes de fazer a chamada de stream');

            stream
            .on("data", async (data) => {
                try {
                const json = JSON.parse(data);
                if (json.connection_issue) {
                    socket.emit("error", json);
                    last_message = {event: 'error', message: json};
                    console.log(new Date(),' --- connection issue');
                    reconnect(stream, socket, token);
                } else 
                {
                    
                    if (json.data) 
                    {
                    socket.emit("tweet", json);
                    last_message = {event: 'tweet', message: json};
                    console.log(json);              

                    // check object fields
                    if ( !( 'matching_rules' in json ) ) {
                        console.log('matching_rules not found in data object');
                        return;
                    }

                    // Check for stream rule
                    const TAG = json.matching_rules[0].tag;
                    let topic = undefined;
                    if ( TAG.indexOf('universe') > -1 ) {
                        topic = 'twitter.universe'
                    } else if ( TAG.indexOf('programming') > -1 ) {
                        topic = 'twitter.programming';
                    } else if ( TAG.indexOf('games') > -1 ) {
                        topic = 'twitter.games';
                    } else if ( TAG.indexOf('devjobs') > -1 ) {
                        topic = 'twitter.devjobs';
                    } else if ( TAG.indexOf('carracing') > -1 ) {
                        topic = 'twitter.carracing';
                    }

                    if ( !topic ) {
                        console.log('topic not found');
                        return;
                    }

                    // Send data to kafka
                    await producer.send({
                        topic: topic,
                        messages: [
                        { value: JSON.stringify(json.data) },
                        ],
                    })

                    console.log('salvou com sucesso no kafka');
                    
                    } else {
                        socket.emit("authError", authMessage);
                        last_message = {event: 'authError', message: authMessage};
                        console.log(new Date(),' --- authError');
                    }
                }
                } catch (e) {
                    socket.emit("heartbeat");
                    last_message = {event: 'heartbeat', message: {}};
                    console.log(new Date(),' --- heartbeat');
                }
            })
            .on("error", (error) => {
                // Connection timed out
                socket.emit("error", errorMessage);
                last_message = {event: 'heartbeat', message: {}};
                console.log(new Date(),' --- connection timed out');
                reconnect(stream, socket, token);
            });
        } catch (e) {
            socket.emit("authError", authMessage);
            console.log(new Date(),' --- authError');
            
            // Disconnect from producer
            await producer.disconnect()
        }
    };

    const sleep = async (delay) => {
        return new Promise((resolve) => setTimeout(() => resolve(true), delay));
    };


    const reconnect = async (stream, socket, token) => {
        timeout++;
        stream.abort();
        await sleep(2 ** timeout * 1000);
        streamTweets(socket, token);
    };

    // Startup with opening streaming connection
    //io.on("connection", async (socket) => {
        try {
            const token = BEARER_TOKEN;
            //io.emit("connect", "Client connected");
            const stream = streamTweets(io, token);
        } catch (e) {
            //io.emit("authError", authMessage);
        }
    //});

}