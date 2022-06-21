const express      = require('express');
const router       = express.Router();
const socketIo     = require("socket.io");
const http         = require("http");
const { Kafka }    = require('kafkajs');

const defaultTopic = 'twitter.universe.analytics';

// routes
router.get('/', getDefaultTopic);
router.get('/:id', getTopic);

module.exports = router;


// route functions 

function getDefaultTopic(req, res, next) {
    twitterAnalytics(defaultTopic);
}

function getTopic(req, res, next) {
    twitterAnalytics( `${req.params.id}.analytics` );
}


// default function
async function twitterAnalytics(topic) {

    let timeout = 0;

    const subscribeError = {
        title: "Subscribe error",
        detail: "Was not possible to subscribe on kafka topic. Reconnecting ...",
    };

    const consumerEachMessageError = {
        title: "Kafka consumer error",
        details: [
            `Error gathering messages from kafka consumer. We will try again ...`,
        ],
        type: "https://developer.twitter.com/en/docs/authentication",
    };

    const app = express();

    app.use(express.json());
    app.use(express.urlencoded({ extended: true }));

    const server = http.createServer(app);
    const io = socketIo(server);


    // Kafka client setup
    const kafka = new Kafka({
        clientId: 'ghg-twitter-stream-listener',
        brokers: ['localhost:9093'],
    });

    const consumer = kafka.consumer({ groupId: 'twitter-analytics-consumer' })


    // Kafka Producer
    const streamAnalytics = async (socket, topic) => {        

        // Connect to Kafka consumer    
        try 
        {
            await consumer.connect();
        
        } catch(e) 
        { 
            socket.emit("consumerConnectError", e);
        }

        // Subscribe to topic
        try {
            await consumer.subscribe({ topics: [topic] });            

        } catch (e) {
            socket.emit("subscribeError", subscribeError);

            await consumer.disconnect();
            reconnect(socket);
        }

        // Consuming analytics messages
        try {
            await consumer.run({
                eachMessage: async ({ topic, partition, message, heartbeat }) => {
                    console.log({
                        key: message.key.toString(),
                        value: message.value.toString(),
                        headers: message.headers,
                    });

                    // send message to client
                    socket.emit("analytic", message);
                },
            })

        } catch (e) {
            socket.emit("consumerEachMessageError", consumerEachMessageError);

            await consumer.disconnect();
            reconnect(socket);
        }
    }


    const sleep = async (delay) => {
        return new Promise((resolve) => setTimeout(() => resolve(true), delay));
    };

    // reconnect on kafka consumer
    const reconnect = async (socket) => {
        timeout++;
        await sleep(2 ** timeout * 1000);
        streamAnalytics(socket, topic);
    };

    // Startup with client side connection
    io.on("connection", async (socket) => {
        
        io.emit("connect", "Client connected");
        const stream = streamAnalytics(io, topic);
        
    });
}