const express    = require("express");
const app        = express();
const path       = require("path");
const http       = require("http");
const cors       = require('cors');
const socketIo     = require("socket.io");
const { Kafka }    = require('kafkajs');
require('dotenv').config();


let port = process.env.PORT || 3005;


app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());


if (process.env.NODE_ENV === "production") {
  app.use(express.static(path.join(__dirname, "../build")));
  app.get("*", (request, res) => {
    res.sendFile(path.join(__dirname, "../build", "index.html"));
  });
} else {
  port = 3005;
}

//app.use('/socket.io', require('./tweets/tweets.controller'))


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


const server = http.createServer(app);
const io = socketIo(server);


// Kafka client setup
const kafka = new Kafka({
    clientId: 'ghg-twitter-stream-listener',
    brokers: ['localhost:9093'],
});

const consumer = kafka.consumer({ groupId: 'twitter-consumer' })


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
        reconnect(socket, topic);
    }

    // Consuming analytics messages
    try {
        await consumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat }) => {
                /*console.log({
                    key: (message.key) ? message.key.toString(): '',
                    value: message.value.toString(),
                    headers: message.headers,
                });*/

                // send message to client
                socket.emit("tweet", {data: JSON.parse(message.value.toString())});
                console.log('after socket emit -- tweet', message.value.toString());
            },
        })

    } catch (e) {
        socket.emit("consumerEachMessageError", consumerEachMessageError);

        await consumer.disconnect();
        reconnect(socket, topic);
    }
}


const sleep = async (delay) => {
    return new Promise((resolve) => setTimeout(() => resolve(true), delay));
};

// reconnect on kafka consumer
const reconnect = async (socket, topic) => {
    timeout++;
    await sleep(2 ** timeout * 1000);
    streamAnalytics(socket, topic);
};

// Startup with client side connection
io.on("connection", async (socket) => {
    
    io.emit("connect", "Client connected");
    const stream = streamAnalytics(io, 'twitter.universe');
    
});

console.log("API ... NODE_ENV is", process.env.NODE_ENV);

// start server
server.listen(port, () => console.log(`API ... Listening on port ${port}`));