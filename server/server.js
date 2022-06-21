const express = require("express");
const app     = express();
const path    = require("path");
const http    = require("http");
const cors    = require('cors');
require('dotenv').config();

const stream = require('./twitterStreamApi');


let port = process.env.PORT || 3000;


app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());


if (process.env.NODE_ENV === "production") {
  app.use(express.static(path.join(__dirname, "../build")));
  app.get("*", (request, res) => {
    res.sendFile(path.join(__dirname, "../build", "index.html"));
  });
} else {
  port = 3001;
}

// api routes
app.use('/analytics', require('./twitterAnalytics'));


console.log("NODE_ENV is", process.env.NODE_ENV);

// start server
app.listen(port, () => console.log(`Listening on port ${port}`));


// execute listener code
stream.twitterStreamApi();