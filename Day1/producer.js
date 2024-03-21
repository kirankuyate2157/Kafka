const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();
  console.log("Connecting Producer....");
  await producer.connect();
  console.log("producer connected!");

  rl.setPrompt(">> ");
  rl.prompt();
  rl.on("line", async (line) => {
    const [rider, location] = line.split(" ");

    await producer.send({
      topic: "driver-updates",
      messages: [
        {
          partition: location.toLowerCase() == "n" ? 0 : 1,
          key: "location-update",
          value: JSON.stringify({ name: rider, loc: location }),
        },
      ],
    });
  }).on("close", async () => {
    await producer.disconnect();
    console.log("producer disconnected!");
  });
}

init();
