package com.c8y.notification;

public class Main {
    private static final String PASSWORD = "Sag@1234";
    private static final String USER = "Manaswi";
    private static final String PROVIDER_URL = "https://manaswiembed.cumulocity.com";

    private static final String SUBSCRIPTION_NAME = "hackathonSubscription";

    public static void main(String[] args) {
        Notification2Wrapper subscription = null;
        try {
            subscription = new Notification2Wrapper(SUBSCRIPTION_NAME, PROVIDER_URL, USER, PASSWORD)
                    .events(Notification2Wrapper.EventType.ALARMS)
//                    .device(Notification2Wrapper.ALL_DEVICES)
                    .device("16412")
//                    .type(Notification2Wrapper.ALL_TYPES)
                    .type("ProgramStatusChanged")
                    .initialize();

            startProcessingMessages();

        } catch (Exception e) {
            e.printStackTrace();
            if (subscription != null) {
// TODO
//                subscription.unsubscribe();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
// TODO
//                subscription.closeWebsocket(false);
            }
        }
    }

    private static void startProcessingMessages() {
        System.out.println("Starting processing messages...");
    }
}