const KafkaConnection = require('../kafka.js');
const { Partitioners } = require('kafkajs')
const { v4: uuidv4 } = require('uuid')
const producer = KafkaConnection.producer({
    transactionTimeout: 30000,
    createPartitioner: Partitioners.LegacyPartitioner,
});

async function publish_to_kafka(count) {

    await producer.connect()
    await producer.send({
        topic: 'pubsub_topic',
        messages: produce_data(count)
    })
}

function produce_data(count){
    let data = [];
    for (let index = 0; index < count; index++) {
        data.push({ 
            key: (Math.floor(Math.random() * 3)).toString(), 
            value : ("Hi from "+ uuidv4()).toString(),
            partition : (Math.floor(Math.random() * 3)).toString()
        });
    }
    console.log(data);
    return data;
}


module.exports.publish_to_kafka = publish_to_kafka;