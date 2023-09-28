package com.c8y.notification;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

public class Notification2Wrapper {
    private static final JSONParser JSON_PARSER = new JSONParser();
    private final String subscriptionName;
    private final String providerUrl;
    private final String authorization;

    private String deviceId = ALL_DEVICES;
    private Contexts context = Contexts.TENANT;
    private EventType eventType = EventType.ALL;
    private String typeFilter = ALL_TYPES;

    private JSONObject subscription = new JSONObject();
    private boolean keepAlive = false;

    enum Contexts {
        MO("mo"),
        TENANT("tenant");
        private String value;

        private Contexts(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    enum EventType {
        ALL("*"),
        ALARMS("alarms"),
        EVENTS("events"),
        MEASUREMENTS("measurements"),
        INVENTORY("managedobjects"),
        OPERATIONS("operations");

        private String value;

        private EventType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    static final String ALL_DEVICES = "*";
    static final String ALL_TYPES = "*";

    public Notification2Wrapper(String subscriptionName, String providerUrl, String user, String password) {
        this.subscriptionName = subscriptionName;
        this.providerUrl = providerUrl;

        String authString = user + ":" + password;
        String authStringEnc = Base64.getEncoder().encodeToString(authString.getBytes());
        this.authorization = "Basic " + authStringEnc;

    }

    public Notification2Wrapper events(EventType eventType) {
        this.eventType = eventType;
        if (!EventType.ALARMS.equals(this.eventType) && !EventType.INVENTORY.equals(this.eventType) && !EventType.ALL.equals(this.eventType)) {
            this.context = Contexts.MO;
        }
        return this;
    }

    public Notification2Wrapper device(String deviceId) {
        if (ALL_DEVICES.equals(deviceId)) {
            this.deviceId = ALL_DEVICES;
            this.context = Contexts.TENANT;
            if (!EventType.ALARMS.equals(this.eventType) && !EventType.INVENTORY.equals(this.eventType)) {
                this.eventType = EventType.ALL;
            }
        } else {
            this.deviceId = deviceId;
            this.context = Contexts.MO;
        }
        return this;
    }

    public Notification2Wrapper type(String typeFilter) {
        this.typeFilter = typeFilter;
        return this;
    }

    public Notification2Wrapper initialize() throws Exception {
        System.out.println("Searching existing subscription named '" + this.subscriptionName + "'...");
        JSONObject existingSubscription = doFindSubscription(this.subscriptionName, this.providerUrl, this.authorization, 1);
        System.out.println("... found subscription: " + existingSubscription.toString());


        Object id = existingSubscription.get("id");
        if (id != null) {
            doDeleteSubscription(this.providerUrl, this.authorization, id.toString());
        }

        System.out.println("Creating new subscription...");
        this.subscription = doCreateSubscription(this.subscriptionName, this.providerUrl, this.authorization, this.context, this.eventType, this.deviceId, this.typeFilter);
        System.out.println("... created subscription: " + this.subscription.toString());
        this.keepAlive = false;

        return this;
    }

    private JSONObject doFindSubscription(String subscriptionName, String providerUrl, String authorization, int currentPage) throws Exception {
        System.out.println("Searching for subscription '" + subscriptionName + "' in page " + currentPage);
        System.out.println(authorization);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(providerUrl + "/notification2/subscriptions?pageSize=2&withTotalPages=true&currentPage=" + currentPage))
                .header("Authorization", authorization)
                .method("GET", HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        int statusCode = response.statusCode();
        if (statusCode != 200) {
            System.out.println("Satus code: " + statusCode);
            throw new Exception("The Notification 2.0 API does not seem to be installed or active on the Cumulocity IoT server. Please check the server configuration.");
        } else {
            JSONObject JSONbody = (JSONObject) JSON_PARSER.parse(response.body());
            JSONArray subscriptions = (JSONArray) JSONbody.get("subscriptions");

            JSONObject subscription = (JSONObject) subscriptions.stream()
                    .filter(s -> subscriptionName.equals(((JSONObject) s).get("subscription").toString()))
                    .findFirst().orElse(null);
            if (subscription != null) {
                return subscription;
            } else {
                // If the subscription is not found, search for it in the following pages
                int totalPages = Integer.parseInt(((JSONObject) JSONbody.get("statistics")).get("totalPages").toString());
                if (currentPage < totalPages) {
                    // Last page not reached -> recursive search
                    return doFindSubscription(subscriptionName, providerUrl, authorization, ++currentPage);
                } else {
                    return new JSONObject();
                }
            }
        }
    }

    private int doDeleteSubscription(String providerUrl, String authorization, String subscriptionId) throws Exception {
        String url = providerUrl + "/notification2/subscriptions/" + subscriptionId;
        System.out.println("Deleting " + url);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authorization)
                .method("DELETE", HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        return response.statusCode();
    }

    private JSONObject doCreateSubscription(String subscriptionName, String providerUrl, String authorization, Contexts context, EventType eventType, String deviceId, String typeFilter) throws Exception {
        JSONObject JSONpayload = new JSONObject();
        JSONpayload.put("context", context.getValue());
        JSONpayload.put("subscription", subscriptionName);
        JSONArray JSONapis = new JSONArray();
        JSONapis.add(eventType.getValue());
        JSONObject JSONsubscriptionFilter = new JSONObject();
        JSONsubscriptionFilter.put("apis", JSONapis);
        if (!ALL_TYPES.equals(typeFilter)) {
            JSONsubscriptionFilter.put("typeFilter", typeFilter);
        }
        JSONpayload.put("subscriptionFilter", JSONsubscriptionFilter);

        if (!ALL_DEVICES.equals(deviceId)) {
            JSONObject JSONsource = new JSONObject();
            JSONsource.put("id", deviceId);
            JSONpayload.put("source", JSONsource);
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(providerUrl + "/notification2/subscriptions"))
                .header("Authorization", authorization)
                .header("Content-Type", "application/json")
                .header("Accept", "application/vnd.com.nsn.cumulocity.subscription+json")
                .method("POST", HttpRequest.BodyPublishers.ofString(JSONpayload.toString()))
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        int statusCode = response.statusCode();
        if (statusCode == 201) {
            return (JSONObject) JSON_PARSER.parse(response.body());
        } else {
            throw new Exception("Subscription with the following payload failed with error " + statusCode +
                    ":\n " + JSONpayload);
        }
    }
}
