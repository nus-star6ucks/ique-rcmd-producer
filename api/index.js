const app = require("express")();
const cors = require("cors");
const { produce } = require("../producer");

app.use(cors());
app.set("port", process.env.PORT || 8081);
app.get("/api/sendUserData", (req, res) => {
  const { userId, latitude, longitude } = req.query;
  if (!userId || !latitude || !longitude)
    return res.json({ success: false, msg: "invalid input" });
  produce(req.query.userId, {
    userId,
    latitude: +latitude,
    longitude: +longitude,
  })
    .then(() => {
      console.log("success");
      res.json({ success: true, msg: "send ok" });
    })
    .catch((error) => {
      console.log("fail", error);
      res.json({ success: false, msg: error });
    });
});

app.listen(app.get("port"), async () => {
  console.log("Server running " + app.get("port"));
});


module.exports = app;
