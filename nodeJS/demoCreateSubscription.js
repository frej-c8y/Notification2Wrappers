const Notification2wrapper = require("./notification2wrapper");
const Promise = require("promise");
const _ = require("lodash");

const {providerUrl, user, password} = require("./credentials")

let msgCount = 0;

let subscription = undefined;
(async function () {
  // // Creation of a Service User
  // // let { name: serviceUsername, password: servicePassword } = await createServiceUser(providerUrl, user, password, "wmio-connector-app");
  // let serviceUsername = user;
  // let servicePassword = password;
  const credentials = `${user}:${password}`;
  const credentials_base64 = Buffer.from(credentials);

  const authHeader = `Basic ${credentials_base64}`;

  try {
    const subscriptionName = "hackathonSubscription";

    subscription = await new Notification2wrapper(subscriptionName, providerUrl, user, password, console)
      .events(Notification2wrapper.eventTypes.ALARMS)
      // .device(Notification2wrapper.ALL_DEVICES)
      .device(16412)
      // .type(Notification2wrapper.ALL_TYPES)
      // .type("ProgramStatusChanged");
      .initialize();
    
    // console.log('Subscription: ', JSON.stringify(subscription.subscription, null, '  '));

    startProcessingMessages();
  } catch (err) {
    console.log("Error catched in caller:", err);
    if (subscription) {
      subscription.unsubscribe();
      await sleep(5000);
      subscription.closeWebsocket(false);
    }
  }
})();

function startProcessingMessages() {
  subscription.openWebsocket(processMessageCallback, afterCloseCallback);
}

async function processMessageCallback(message) {
  // For test purposes: we stop as soon as at least one message is received and
  // recreate a subscription and a websocket immediately afterwards
  //subscription.closeWebsocket(true);

  let notificationEventType = message.notificationHeaders[0].split("/")[2];
 
//   message = _.pick(message, [ "ackHeader", "notificationHeaders[0]", "message.creationTime", "message.type", "message.severity", ]);
  console.log(`Message ${msgCount}:${JSON.stringify(message, null, 4)}`);

  const dateDiffInSec = (new Date().getTime() - Date.parse(message.message.time)) / 1000;
  console.log(`Message is ${dateDiffInSec} sec old`);
  msgCount++;

/*  if (dateDiffInSec < 90 &&
    (notificationEventType != Wrappernotification2wrapperALARMS ||
      (notificationEventType === Wrappernotification2wrapperALARMS && message.message.severity === Wrappernotification2wrapperJOR)) &&
    msgCount > 2
  ) {*/
    // For test purposes: we stop as soon as one message / one alarm with severity MAJOR is received
    subscription.unsubscribe();
    // Allow 5s extra time before closing the websocket to consume and acknowledge further pending incoming messages
    await sleep(5000);
    subscription.closeWebsocket(false);
//  }
}

async function afterCloseCallback() {
  console.log(`  Waiting for 10 sec before restarting...`);
  await sleep(10000);
  console.log(`  -> Restart...`);

  startProcessingMessages();
}

function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}