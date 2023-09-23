const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'] // Add the addresses of your Kafka brokers here
});

const producer = kafka.producer();

// Example usage



async function produceMessage(topic, message) {
    // give some random sentences
    const messages = [
        "The quick brown fox jumps over the lazy dog",
        "The five boxing wizards jump quickly",
        "The quick onyx goblin jumps over the lazy dwarf",
        "The Beefy wizard jumps quickly",
        ];
    await producer.connect();
    for(let i = 0; i < messages.length; i++){
        await producer.send({
            topic,
            messages: [
                {
                    value: messages[i]
                }
            ]
        });
    }
    await producer.disconnect();
}

module.exports = produceMessage;