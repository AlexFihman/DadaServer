package com.fihman.dada;

import org.eclipse.paho.client.mqttv3.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.boot.CommandLineRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class DistributedNode {
    private static final Logger logger = LoggerFactory.getLogger(DistributedNode.class);

    private static final String ELECTION_TOPIC = "db/election";
    private static final String MASTER_TOPIC = "db/master";
    private static final String NODES_TOPIC = "db/nodes";
    private static final String NODES_HEARTBEAT = "db/heartbeat";
    private static final String COMMAND_TOPIC_PREFIX = "db/node/";
    private static final String RESPONSE_TOPIC = "db/master/response";
    private boolean isMaster = false;
    private boolean electionInProgress = false;
    private final MqttUtils mqttUtils;
    private final String nodeId;

    private MqttClient client;

    @Autowired
    public DistributedNode(MqttUtils mqttUtils) {
        this.mqttUtils = mqttUtils;
        this.nodeId = String.valueOf(NodeIdProvider.getNodeId());
        start();
    }



    public void start() {
        try {
            client = mqttUtils.connectToBroker(nodeId);

            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    logger.error("Connection lost!");
                    try {
                        client = mqttUtils.connectToBroker(nodeId);
                    } catch (Exception ex) {
                        logger.error("connectionLost", ex);
                    }
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    handleMessage(topic, new String(message.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });

            client.subscribe(ELECTION_TOPIC);
            client.subscribe(MASTER_TOPIC);
            client.subscribe(COMMAND_TOPIC_PREFIX + nodeId);
            client.subscribe(NODES_HEARTBEAT); // Subscribe to heartbeat topic

            registerNode();
            monitorHeartbeat(); // Start monitoring heartbeats
        } catch (Exception e) {
            logger.error("Start", e);
        }
    }

    private void monitorHeartbeat() {
        Timer heartbeatMonitor = new Timer(true);
        heartbeatMonitor.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (electionInProgress)
                    return;
                long time = System.currentTimeMillis();
                if (!isMaster && time - lastHeartbeatTime > 90000 && time > lastHeartbeatIgnoreTime) {
                    logger.warn("Master seems to have died. Starting new election.");
                    lastHeartbeatIgnoreTime = time + 600000; // 10 min
                    startElection();
                }
            }
        }, 0, 10000); // Check every 10 seconds
    }

    private void registerNode() {
        String nodeInfo = "REGISTER:" + nodeId;
        publish(NODES_TOPIC, nodeInfo);
    }

    private void startElection() {
        logger.info("Starting election...");
        electionInProgress = true;
        publish(ELECTION_TOPIC, "ELECTION:" + nodeId);

        // Wait for election results or declare self as master
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                if (electionInProgress) {
                    declareMaster();
                }
            }
        }, 5000);
    }

    private long lastHeartbeatTime = System.currentTimeMillis();
    private long lastHeartbeatIgnoreTime = -1;

    private void handleMessage(String topic, String message) {
        if (topic.equals(NODES_HEARTBEAT)) {
            lastHeartbeatTime = System.currentTimeMillis();
        } else if (topic.equals(ELECTION_TOPIC)) {
            handleElectionMessage(message);
        } else if (topic.equals(MASTER_TOPIC)) {
            handleMasterAnnouncement(message);
        } else if (topic.startsWith(COMMAND_TOPIC_PREFIX)) {
            handleCommand(message);
        }
    }

    private void handleElectionMessage(String message) {
        String receivedId = message.split(":")[1];
        if (receivedId.compareTo(nodeId) > 0) {
            lastHeartbeatIgnoreTime = System.currentTimeMillis() + 600000;
            logger.info("Another node (\"{}\") is more eligible. Standing down.", receivedId);
            electionInProgress = false;
        } else if (receivedId.compareTo(nodeId) < 0) {
            logger.info("I am more eligible than \"{}\". Sending my ID.", receivedId);
            publish(ELECTION_TOPIC, "ELECTION:" + nodeId);
        }
    }

    private void handleMasterAnnouncement(String message) {
        logger.info("New master announced: {}", message);
        isMaster = message.equals(nodeId);
        electionInProgress = false;
    }

    private void declareMaster() {
        logger.info("Declaring self as master.");
        isMaster = true;
        publish(MASTER_TOPIC, nodeId);
        startHeartbeat();
    }
    private void startHeartbeat() {
        Timer heartbeatTimer = new Timer(true);
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (isMaster) {
                    publish("db/heartbeat", nodeId);
                }
            }
            //todo: parameter
        }, 0, 60000); // Send heartbeat every 60 seconds
    }

    private void handleCommand(String command) {
        if (command.equals("GET_CPU_INFO")) {
            logger.info("Received command: GET_CPU_INFO");
            String cpuInfo = getCpuInfo();
            publish(RESPONSE_TOPIC, nodeId + ":CPU_INFO:" + cpuInfo);
        }
    }

    private String getCpuInfo() {
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        int availableProcessors = osBean.getAvailableProcessors();
        double systemLoad = osBean.getSystemLoadAverage();
        return "CORES:" + availableProcessors + ",LOAD:" + systemLoad;
    }

    private void publish(String topic, String message) {
        try {
            MqttMessage mm = new MqttMessage(message.getBytes());
            mm.setQos(0);
            client.publish(topic, mm);
        } catch (MqttException e) {
            logger.error("Publish error", e);
        }
    }
}