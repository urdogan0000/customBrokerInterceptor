package com.liderahenk.custombroker;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.intercept.InterceptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.util.Map;

public class CustomBrokerInterceptor implements BrokerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(CustomBrokerInterceptor.class);
    private static final String ONLINE_TOPIC = "online";
    private static final String OFFLINE_TOPIC = "offline";

    private static volatile PulsarService pulsarService;
    private static volatile Producer<OnlineStatusMessageDTO> onlineProducer;
    private static volatile Producer<OnlineStatusMessageDTO> offlineProducer;

    private static void setPulsarService(PulsarService pulsarService) {
        CustomBrokerInterceptor.pulsarService = pulsarService;
    }

    private synchronized Producer<OnlineStatusMessageDTO> createProducer(String topicName) {
        try {
            PulsarClient client = pulsarService.getClient();
            Producer<OnlineStatusMessageDTO> producer = client.newProducer(Schema.JSON(OnlineStatusMessageDTO.class))
                    .topic(topicName)
                    .create();
            log.info("Producer created successfully for topic {} with OnlineStatusMessageDTO schema.", topicName);
            return producer;
        } catch (Exception e) {
            log.error("Producer initialization failed: {}", e.getMessage(), e);
            return null;
        }
    }

    private synchronized void initializeProducer() {
        if (onlineProducer == null) {
            onlineProducer = createProducer(ONLINE_TOPIC);
        }
        if (offlineProducer == null) {
            offlineProducer = createProducer(OFFLINE_TOPIC);
        }
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx) {

        log.info("New client connection established from IP: {}", cnx.clientAddress());
        log.info("Client IP and source information: {}", cnx.clientSourceAddressAndPort());
    }

    @Override
    public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
        BrokerInterceptor.super.consumerCreated(cnx, consumer, metadata);
        log.info("Consumer connected with subscriptionName: {}", consumer.getSubscription().getName());

        if (onlineProducer == null) {
            initializeProducer();
        }

        if (onlineProducer != null) {
            OnlineStatusMessageDTO message = new OnlineStatusMessageDTO(consumer.getSubscription().getName());
            try {
                onlineProducer.sendAsync(message)
                        .thenAccept(msgId -> log.info("Reactive message sent to online topic {} with ID: {}", ONLINE_TOPIC, msgId))
                        .exceptionally(e -> {
                            log.error("Failed to send reactive message to online topic: {}", e.getMessage(), e);
                            return null;
                        });
                log.info("Message sent to online topic {} from CustomBrokerInterceptor.", ONLINE_TOPIC);
            } catch (Exception e) {
                log.error("Failed to send message from CustomBrokerInterceptor: {}", e.getMessage(), e);
            }
        } else {
            log.warn("Online producer is not initialized. Message not sent for consumer with ID: {}", consumer.getSubscription().getName());
        }
    }

    @Override
    public void consumerClosed(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
        BrokerInterceptor.super.consumerClosed(cnx, consumer, metadata);
        log.info("Consumer closed with subscriptionName: {}", consumer.getSubscription().getName());

        if (offlineProducer == null) {
            initializeProducer();
        }

        if (offlineProducer != null) {
            OnlineStatusMessageDTO message = new OnlineStatusMessageDTO(consumer.getSubscription().getName());
            try {
                offlineProducer.sendAsync(message)
                        .thenAccept(msgId -> log.info("Reactive message sent to offline topic {} with ID: {}", OFFLINE_TOPIC, msgId))
                        .exceptionally(e -> {
                            log.error("Failed to send reactive message to offline topic: {}", e.getMessage(), e);
                            return null;
                        });
                log.info("Message sent to offline topic {} from CustomBrokerInterceptor.", OFFLINE_TOPIC);
            } catch (Exception e) {
                log.error("Failed to send message from CustomBrokerInterceptor: {}", e.getMessage(), e);
            }
        } else {
            log.warn("Offline producer is not initialized. Message not sent for consumer with ID: {}", consumer.getSubscription().getName());
        }
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
        // Implement if needed
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        log.info("Connection Closed from IP: {}", cnx.clientAddress());
        log.info("Logout Connection Client IP and source information: {}", cnx.clientSourceAddressAndPort());
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {
        // Implement if needed
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response) throws IOException, ServletException {
        // Implement if needed
    }

    @Override
    public synchronized void initialize(PulsarService pulsarService) {
        log.info("Initializing CustomBrokerInterceptor...");
        CustomBrokerInterceptor.setPulsarService(pulsarService);
        initializeProducer();
    }

    @Override
    public void close() {
        try {
            if (onlineProducer != null) {
                onlineProducer.close();
                log.info("Online producer closed.");
            }
            if (offlineProducer != null) {
                offlineProducer.close();
                log.info("Offline producer closed.");
            }
        } catch (Exception e) {
            log.error("Failed to close producer: {}", e.getMessage(), e);
        }
    }
}
