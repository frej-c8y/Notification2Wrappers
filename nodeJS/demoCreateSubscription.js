const Notification2wrapper = require("./notification2wrapper");
const Promise = require("promise");
const request = require("request");
const _ = require("lodash");

const {providerUrl, user, password} = require("./credentials")

let msgCount = 0;

let subscription = undefined;
let subscription1 = undefined;

(async function () {
  const credentials = `${user}:${password}`;
  const credentials_base64 = Buffer.from(credentials);

  const authHeader = `Basic ${credentials_base64}`;

  try {
    const subscriptionName = "hackathonSubscription";

    subscription = await new Notification2wrapper(subscriptionName, providerUrl, user, password, console)
      .events(Notification2wrapper.eventTypes.ALARMS)
      // .device(Notification2wrapper.ALL_DEVICES)
        .device(941587)                // "E-xyz Windturbine #1" on enterprise1.dtm.stage.c8y.io
      // .device(603532262)           // "m52xq" on enterprise1.dtm.stage.c8y.io
      // .device(16412)                // on manaswiembed
      // .device(74409429)             // on oee-staging
      // .type(Notification2wrapper.ALL_TYPES)
      // .type("ProgramStatusChanged");
      .initialize();

    subscription1 = await new Notification2wrapper(subscriptionName, providerUrl, user, password, console)
        .events(Notification2wrapper.eventTypes.ALARMS)
        .device(603532262)          // "m52xq" on enterprise1.dtm.stage.c8y.io
      .initialize(false);

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
  subscription.openWebsocket(processMessageCallback);
}

async function processMessageCallback(message) {
  // For test purposes: we unsubscribe and close the websocket as soon as at least one message is received

  let notificationEventType = message.notificationHeaders[0].split("/")[2];
 
  console.log(`Message ${msgCount}:${JSON.stringify(message, null, 4)}`);
  await sendEmail(message);

  const dateDiffInSec = (new Date().getTime() - Date.parse(message.message.lastUpdated)) / 1000;
  console.log(`Message is ${dateDiffInSec} sec old`);
  msgCount++;

  // For test purposes: we stop as soon as one message / one alarm with severity MAJOR is received
  subscription.unsubscribe();
  // Allow 5s extra time before closing the websocket to consume and acknowledge pending incoming messages
  await sleep(5000);
  subscription.closeWebsocket(false);
}

function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}

function sendEmail(message) {
//  emailProviderUrl = providerUrl;
//  emailUser = user;
//  emailPassword = password;
  emailProviderUrl = 'https://oee-dev.eu-latest.cumulocity.com';
  emailUser = 'frederic.joulin@softwareag.com';
  emailPassword = 'Jsx:YsDsscf3Zwn';

  let payload = {
    "to": [
      "frederic.joulin@softwareag.com",
      "frederic.joulin@cumulocity.com"
    ],
    "cc": [],
    "replyTo": "frej@softwareag.com",
    "subject": "Message " + message.ackHeader + " received",
    "text": "<pre>" + JSON.stringify(message.message, null, 4) + "</pre>"
  };

  return new Promise(function (resolve, reject) {
    request(
      {
        method: "POST",
        url: emailProviderUrl + '/email/emails',
        auth: {
          user: emailUser,
          pass: emailPassword,
          sendImmediately: true
        },
        json: true,
        body: payload
      },
      function (err, res, body) {
        if (err) {
          reject(err);
        }

        const statusCode = _.get(res, "statusCode", -1);
        switch (statusCode) {
          case 200:
            // console.log("response body: " + JSON.stringify(body));
            console.log("Message successfully sent.");
            resolve(body);
            break;
          default:
            reject(`Email sending failed with status ${statusCode}`);
        }
      }
    );
  });
}