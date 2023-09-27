// Functions for the Notification2 API are currently unavailable in the @c8y/client library
// This class will either
// - have to be replaced with @c8y/client methods once the Notification2 API is integrated into this library
// or
// - serve as a basis for the implementation of the Notification2 API in the @c8y/client library

const _ = require("lodash");
const request = require("request");
const WebSocket = require("ws");

const readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
const contexts = {
  MO: "mo",
  TENANT: "tenant"
};
const eventTypes = {
  ALL: "*",
  ALARMS: "alarms",
  EVENTS: "events",
  MEASUREMENTS: "measurements",
  INVENTORY: "managedobjects",
  OPERATIONS: "operations"
};
const severity = {
  CRITICAL: "CRITICAL",
  MAJOR: "MAJOR",
  MINOR: "MINOR",
  WARNING: "WARNING"
};
const ALL_DEVICES = "*";
const ALL_TYPES = "*";

module.exports = class Notification2Wrapper {
  static get eventTypes() { return eventTypes; }

  static get severity() { return severity; }

  static get ALL_DEVICES() { return ALL_DEVICES; }

  static get ALL_TYPES() { return ALL_TYPES; }

  constructor(name, providerUrl, username, password, customLogger) {
    this.name = name;
    this.providerUrl = providerUrl;
    this.username = username;
    this.password = password;

    this.eventType = eventTypes.ALL;
    this.deviceId = ALL_DEVICES;
    this.context = contexts.TENANT;
    this.typeFilter = ALL_TYPES;

    this.customLogger = customLogger;
  }

  device(deviceId) {
    if (deviceId === ALL_DEVICES) {
      this.deviceId = ALL_DEVICES;
      // All devices selected -> context is automatically set to "contexts.TENANT"
      this.context = contexts.TENANT;
      if (this.eventType != eventTypes.ALARMS && this.eventType != eventTypes.INVENTORY) {
        // All devices selected -> event type  must be either "eventTypes.ALARMS" or "eventTypes.INVENTORY" and is reset to "eventTypes.ALL"
        this.eventType = eventTypes.ALL;
      }
    } else {
      this.deviceId = deviceId.toString();
      // One device selected -> context is automatically set to "contexts.MO"
      this.context = contexts.MO;
    }

    return this;
  }

  events(eventType) {
    this.eventType = eventType;
    if (this.eventType !== eventTypes.ALARMS && this.eventType !== eventTypes.INVENTORY && this.eventType !== eventTypes.ALL) {
      this.context = contexts.MO;
    }

    return this;
  }

  type(typeFilter) {
    this.typeFilter = typeFilter;

    return this;
  }

  async initialize() {
    this.customLogger.log(`Searching existing subscription named '${this.name}'...`);
    let existingSubscription = await doFindSubscription( this.name, this.providerUrl, this.username, this.password, 1, this.customLogger);
    this.customLogger.log('... found subscription:', existingSubscription);
    if (_.has(existingSubscription, "id")) {
      await doDeleteSubscription( this.providerUrl, this.username, this.password, existingSubscription, this.customLogger );
    }

    this.customLogger.log('Creating new subscription...');
    this.subscription = JSON.parse(
      await doCreateSubscription( this.name, this.providerUrl, this.username, this.password, this.context, this.eventType, this.deviceId, this.typeFilter )
    );
    this.customLogger.log('... created subscription:', this.subscription);
    this.keepAlive = false;

    return this;
  }

  async createToken() {
    if (!this.subscription) {
      throw "Unable to create a token before the subscription has been initialized - Execute await Notification2Wrapper.initialize() before attempting to create the token";
    }

    this.token = await doCreateToken( this.providerUrl, this.username, this.password, this.subscription );

    return this;
  }

  async unsubscribe() {
    if (this.subscription) {
      let status = await doDeleteSubscription( this.providerUrl, this.username, this.password, this.subscription, this.customLogger );
      if (status != 204 && status != 404) {
        // 404 in case the subscription has been deleted in the meantime which may occur and is ok
        throw `Subscription deletion failed with code ${status}`;
      }
      this.subscription = undefined;
      this.token = undefined;
    }

    return this;
  }

  async openWebsocket(processMessageCallback) {
    const self = this;

    if (!this.subscription) {
      throw "Unable to create a token before the subscription has been initialized - Execute await Notification2Wrapper.initialize() before attempting to create the token";
    }

    await self.createToken();
    const wsUrl = buildWebsocketUrl(this.providerUrl, this.token);

    this.ws = new WebSocket(wsUrl);

    this.ws.on("open", function open() {
      self.customLogger.log(`WebSocket opened -> client readyState ${readyStates[self.ws.readyState]}`);
    });

    this.ws.on("message", function message(msg, isBinary) {
      self.customLogger.log(`Message received: ${msg.toString()}`);
      try {
        let message = parseNotification(msg.toString());
        processMessageCallback(message);

        // Acknowledge notification
        self.customLogger.log(`Acknowledging message ${message.ackHeader}`);
        self.ws.send(message.ackHeader);
      } catch (error) {
        self.customLogger.log(`Failed to process or acknowledge message ${message.ackHeader} with error ${error}. The message will be reprocessed.`);
      }
    });

    this.ws.on("error", function error(error) {
      self.customLogger.log(`WebSocket error: ${error} -> client readyState now ${readyStates[self.ws.readyState]}`);

      self.keepAlive = true;
      self.ws.terminate();
    });

    this.ws.on("close", async function close(code, reason) {
      self.customLogger.log(`WebSocket closed with code ${code} and reason '${reason}' -> client readyState now ${readyStates[self.ws.readyState]}`);

      if ( code !== 1000 && code !== 1005 ) { // Reopen websocket on any code except regular shutdown
        self.customLogger.log(`  Token renewal and reopening the websocket`);
        await sleep(500);
        self.openWebsocket(processMessageCallback);
      } else {
        if (self.keepAlive) { // Reopen websocket on regular shutdown with explicit keepAlive
          await sleep(500);
          self.openWebsocket(processMessageCallback);
        }
      }
    });

    return this;
  }

  async closeWebsocket(keepAlive = false) {
    this.keepAlive = keepAlive;
    if (this.ws) {
      this.ws.close();
    }
  }
};

async function doFindSubscription(name, providerUrl, username, password, currentPage, customLogger) {
  return new Promise(function (resolve, reject) {
    request(
      {
        url: `${providerUrl}/notification2/subscriptions?pageSize=2&withTotalPages=true&currentPage=${currentPage}`,
        auth: {
          user: username,
          pass: password,
          sendImmediately: false
        }
      },
      function (err, res, body) {
        if (err) {
          reject(err);
        }

        customLogger.debug(`Searching for subscription '${name}' in page ${currentPage}`);
        const statusCode = _.get(res, "statusCode", -1);
        if (statusCode !== 200) {
          // The <providerUrl>/notification2/subscription endpoint should always be available
          // UNLESS the Notification 2.0 API is not installed on the Cumulocity IoT server
          customLogger.debug(`Search for subscription '${name}' in page ${currentPage} returned unexpected status ${statusCode}`);
          reject(`The Notification 2.0 API does not seem to be installed or active on the Cumulocity IoT server. Please check the server configuration.`);
        } else {
          let JSONbody = JSON.parse(body);
          let subscriptions = _.get(JSONbody, "subscriptions", []);
          let subscription = _.find(subscriptions, { subscription: name });
          if (typeof subscription !== "undefined") {
            // Return the subscription if found
            customLogger.debug(`Found subscription '${name}' in page ${currentPage}`);
            resolve(formatSubscription(subscription));
          } else {
            // If the subscription is not found, search for it in the following pages
            let totalPages = _.get(JSONbody, "statistics.totalPages", 1);
            if (currentPage < totalPages) {
              // Last page not reached -> recursive search
              resolve(doFindSubscription(name, providerUrl, username, password, currentPage + 1, customLogger));
            } else {
              // Last page reached -> the subscription does not exist
              resolve({});
            }
          }
        }
      }
    );
  });
}

async function doDeleteSubscription( providerUrl, username, password, subscription, customLogger) {
  const subscriptionId = _.get(subscription, "id");
  customLogger.log(`Deleting ${providerUrl}/notification2/subscriptions/${subscriptionId}`);

  return new Promise(function (resolve, reject) {
    request(
      {
        method: "DELETE",
        url: `${providerUrl}/notification2/subscriptions/${subscriptionId}`,
        auth: {
          user: username,
          pass: password,
          sendImmediately: false
        }
      },
      function (err, res, body) {
        if (err) {
          reject(err);
        }
        resolve(_.get(res, "statusCode", -1));
      }
    );
  });
}

async function doCreateSubscription( name, providerUrl, username, password, context, eventType, deviceId, typeFilter ) {
  let payload = {
    context,
    subscription: name,
    subscriptionFilter: {
      apis: [eventType]
    }
  };
  if (deviceId !== ALL_DEVICES) {
    payload = { ...payload,
      source: {
        id: deviceId
      }
    };
  }
  if (typeFilter !== ALL_TYPES) {
    payload = { ...payload,
      subscriptionFilter: { ...payload.subscriptionFilter, typeFilter }
    };
  }

  return new Promise(function (resolve, reject) {
    request(
      {
        method: "POST",
        url: `${providerUrl}/notification2/subscriptions`,
        auth: {
          user: username,
          pass: password,
          sendImmediately: false
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
          case 201:
            resolve(JSON.stringify(formatSubscription(body)));
            break;
          default:
            reject(`Subscription with the following payload failed with error ${statusCode}:\n${JSON.stringify(payload)}`);
        }
      }
    );
  });
}

async function doCreateToken(providerUrl, username, password, subscription) {
  let subscriptionName = subscription.subscription;
  let payload = {
    subscriber: "wmioSubscriber",
    subscription: subscriptionName,
    expiresInMinutes: 1
  };

  return new Promise(function (resolve, reject) {
    request(
      {
        method: "POST",
        url: `${providerUrl}/notification2/token`,
        auth: {
          user: username,
          pass: password,
          sendImmediately: false
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
            resolve(body.token);
            break;
          default:
            reject(`Token creation failed with status ${statusCode}`);
        }
      }
    );
  });
}

function formatSubscription(subscription) {
  return _.pick(subscription, [ "id", "subscription", "context", "subscriptionFilter", "source.id", "source.name", "fragmentsToCopy"]);
}

function buildWebsocketUrl(host, token) {
  const protocolRegex = /^https?/i;
  let wsUrl = protocolRegex.test(host) ? host.replace(protocolRegex, "wss") : `wss://${host}`;

  return `${wsUrl}/notification2/consumer/?token=${token}`;
}

function parseNotification(message) {
  let headers = [];
  while (true) {
    let i = message.indexOf("\n");
    if (i == -1) {
      break;
    }
    let header = message.substring(0, i);
    message = message.substring(i + 1);
    if (header.length == 0) {
      break;
    }
    headers.push(header);
  }

  let parsedMessage = JSON.parse(message);
  if (headers.length === 0) {
    return { message: parsedMessage };
  }
  return {
    ackHeader: headers[0],
    notificationHeaders: headers.slice(1),
    message: parsedMessage
  };
}

function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}