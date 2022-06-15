const express = require("express");
const bodyParser = require("body-parser");
const util = require("util");
const request = require("request");
const path = require("path");
const socketIo = require("socket.io");
const http = require("http");
const { Kafka } = require('kafkajs');

const app = express();
let port = process.env.PORT || 3000;
const post = util.promisify(request.post);
const get = util.promisify(request.get);

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

const server = http.createServer(app);
const io = socketIo(server);

const BEARER_TOKEN = process.env.TWITTER_BEARER_TOKEN;

let timeout = 0;

const streamURL = new URL(
  "https://api.twitter.com/2/tweets/search/stream?tweet.fields=context_annotations&expansions=author_id"
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
  brokers: ['localhost:9092'],
})


// Kafka Producer
const producer = kafka.producer();


// Stream main Login
const streamTweets = (socket, token) => {
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
  }

  try {
    const stream = request.get(config);

    stream
      .on("data", (data) => {
        try {
          const json = JSON.parse(data);
          if (json.connection_issue) {
            socket.emit("error", json);
            reconnect(stream, socket, token);
          } else 
          {
            if (json.data) 
            {
              socket.emit("tweet", json);

              // Check for stream rule
              const TAG = json.data?.matching_rules[0]?.tag;
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
                return;
              }

              // Send data to kafka
              await producer.send({
                topic: topic,
                messages: [
                  { value: 'Hello KafkaJS user!' },
                ],
              })
            
            } else {
              socket.emit("authError", json);
            }
          }
        } catch (e) {
          socket.emit("heartbeat");
        }
      })
      .on("error", (error) => {
        // Connection timed out
        socket.emit("error", errorMessage);
        reconnect(stream, socket, token);
      });
  } catch (e) {
    socket.emit("authError", authMessage);
    
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
io.on("connection", async (socket) => {
  try {
    const token = BEARER_TOKEN;
    io.emit("connect", "Client connected");
    const stream = streamTweets(io, token);
  } catch (e) {
    io.emit("authError", authMessage);
  }
});


console.log("NODE_ENV is", process.env.NODE_ENV);

if (process.env.NODE_ENV === "production") {
  app.use(express.static(path.join(__dirname, "../build")));
  app.get("*", (request, res) => {
    res.sendFile(path.join(__dirname, "../build", "index.html"));
  });
} else {
  port = 3001;
}

server.listen(port, () => console.log(`Listening on port ${port}`));
