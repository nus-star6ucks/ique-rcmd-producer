const Kafka = require("node-rdkafka");

function configFromEnv() {
  return {
    "bootstrap.servers": process.env.bootstrap_servers,
    "sasl.username": process.env.sasl_username,
    "sasl.password": process.env.sasl_password,
    "security.protocol": process.env.security_protocol,
    "sasl.mechanisms": process.env.sasl_mechanisms,
    session_timeout_ms: process.env.session_timeout_ms,
    dr_msg_cb: true,
  };
}

function createProducer(onDeliveryReport) {
  const producer = new Kafka.Producer(configFromEnv());
  return new Promise((resolve, reject) => {
    producer
      .on("ready", () => {
        resolve(producer);
      })
      .on("delivery-report", onDeliveryReport)
      .on("event.error", (err) => {
        console.warn("event.error", err);
        reject(err);
      });
    producer.connect();
  });
}

async function produce(userId, storeValue) {
  const topic = "user-info";
  const producer = await createProducer((err, report) => {
    if (err) {
      console.warn("Error producing", err);
    } else {
      console.log("Produced event to topic");
    }
  });
  const key = Buffer.from(userId);
  const value = Buffer.from(JSON.stringify(storeValue));

  producer.produce(topic, -1, value, key);
  producer.flush(10000, () => {
    producer.disconnect();
  });
}

module.exports = { produce };
