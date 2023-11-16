const express = require("express");
const app = express();
const producer = require("./producer.js")

app.listen(8080, () => {
	console.log("Server started at port 8080");
});

app.get("/produce/:count", (req, res) => {
    producer.publish_to_kafka(req.params.count);
    res.status(200).send("Producing "+req.params.count+" records");
})

  

