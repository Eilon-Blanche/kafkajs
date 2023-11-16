const KafkaConnection = require('../kafka.js');

const consumer = KafkaConnection.consumer({ groupId: 'test-kafka-app' })

// const value = Buffer.from(items[Math.floor(Math.random() * items.length)]);
async function consumer_fetch_from_kafka() {
  let records_fetched = []
    await consumer.connect()
    await consumer.subscribe({ topic : 'pubsub_topic', fromBeginning: true })
    await consumer.run({
      eachBatchAutoResolve: true,
      eachBatch: async ({ batch }) => {
        console.log("BATCH", batch)
        for (let message of batch.messages) {
          records_fetched.push({"key" : message.key.toString(), "value" : message.value.toString()})
          console.log("KEY : ",message.key.toString(), "VALUE : ",message.value.toString(), "PARTITION", batch.partition);
        }
      }
    })
    return records_fetched;
}

consumer_fetch_from_kafka();