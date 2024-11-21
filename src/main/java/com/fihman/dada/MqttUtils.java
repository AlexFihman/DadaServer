package com.fihman.dada;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

@Component
public class MqttUtils {
    private static final Logger logger = LoggerFactory.getLogger(MqttUtils.class);
    @Value("${broker.url}")
    public String BROKER;

    @Value("${broker.user}")
    public String BROKER_USER;

    @Value("${broker.pwd}")
    public String BROKER_PASSWORD;

    public MqttClient connectToBroker(String clientId) throws MqttException {
        // Logic to handle connecting to the MQTT broker
        logger.info("Connecting to broker at: " + BROKER + " with client ID: " + clientId);
        MqttClientPersistence persistence = new MemoryPersistence();
        MqttClient client = new MqttClient(BROKER, clientId, persistence);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setUserName(BROKER_USER);
        connOpts.setPassword(BROKER_PASSWORD.toCharArray());
        client.connect(connOpts);
        return client;
    }
}
