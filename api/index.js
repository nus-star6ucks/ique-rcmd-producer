const app = require("express")();
const cors = require("cors");
app.use(cors());

app.set("port", process.env.PORT || 8081);

app.get("/api/sendUserData", (req, res) => {
  const value = req.query;
  produceExample(req.query.userId, value)
    .then(() => {
      console.log("success");
      res.json({ success: true, msg: "send ok" });
    })
    .catch((error) => {
      console.log("fail", error);
      res.json({ success: false, msg: error });
    });
});

app.listen(app.get("port"), () =>
  console.log("Server running " + app.get("port"))
);

const Kafka = require("node-rdkafka");
const { configFromPath } = require("./util");

function createConfigMap(config) {
  if (config.hasOwnProperty("security.protocol")) {
    return {
      "bootstrap.servers": config["bootstrap.servers"],
      "sasl.username": config["sasl.username"],
      "sasl.password": config["sasl.password"],
      "security.protocol": config["security.protocol"],
      "sasl.mechanisms": config["sasl.mechanisms"],
      dr_msg_cb: true,
    };
  } else {
    return {
      "bootstrap.servers": config["bootstrap.servers"],
      dr_msg_cb: true,
    };
  }
}

function createProducer(config, onDeliveryReport) {
  const producer = new Kafka.Producer(createConfigMap(config));
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

async function produceExample(userId, storeValue) {
  if (process.argv.length < 3) {
    console.log(
      "Please provide the configuration file path as the command line argument"
    );
    process.exit(1);
  }
  let configPath = process.argv.slice(2)[0];
  const config = await configFromPath(configPath);

  const topic = "user-info";
  const producer = await createProducer(config, (err, report) => {
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

module.exports = app;
