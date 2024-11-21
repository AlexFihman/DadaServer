package com.fihman.dada;

import org.eclipse.paho.client.mqttv3.*;

import java.util.HashSet;
import java.util.Set;
public class MasterNode {

    private static final String NODES_TOPIC = "db/nodes";
    private static final String COMMAND_TOPIC_PREFIX = "db/node/";
    private static final String RESPONSE_TOPIC = "db/master/response";

    private Set<String> nonMasterNodes = new HashSet<>();
    private MqttClient client;

//    public static void main(String[] args) {
//        //new MasterNode().start();
//    }

//    public void start() {
//        try {
//            client = new MqttClient(BROKER, "MasterNode");
//            client.connect();
//
//            client.setCallback(new MqttCallback() {
//                @Override
//                public void connectionLost(Throwable cause) {
//                    System.out.println("Connection lost!");
//                }
//
//                @Override
//                public void messageArrived(String topic, MqttMessage message) {
//                    handleMessage(topic, new String(message.getPayload()));
//                }
//
//                @Override
//                public void deliveryComplete(IMqttDeliveryToken token) {}
//            });
//
//            // Subscribe to nodes and response topics
//            client.subscribe(NODES_TOPIC);
//            client.subscribe(RESPONSE_TOPIC);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    private void handleMessage(String topic, String message) {
        if (topic.equals(NODES_TOPIC)) {
            handleNodeRegistration(message);
        } else if (topic.equals(RESPONSE_TOPIC)) {
            System.out.println("Response from node: " + message);
        }
    }

    private void handleNodeRegistration(String message) {
        if (message.startsWith("REGISTER:")) {
            String nodeId = message.split(":")[1];
            String status = message.split(":")[2];
            if (!status.equals("MASTER")) {
                nonMasterNodes.add(nodeId);
                System.out.println("Registered node: " + nodeId);
            }
        }
    }

    public void sendCpuInfoRequest() {
        for (String nodeId : nonMasterNodes) {
            publish(COMMAND_TOPIC_PREFIX + nodeId, "GET_CPU_INFO");
        }
    }

    private void publish(String topic, String message) {
        try {
            client.publish(topic, new MqttMessage(message.getBytes()));
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
