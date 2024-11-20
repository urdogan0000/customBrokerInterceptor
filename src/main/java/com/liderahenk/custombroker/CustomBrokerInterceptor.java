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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CustomBrokerInterceptor implements BrokerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(CustomBrokerInterceptor.class);
    private static final String ONLINE_TOPIC = "online";
    private static final String OFFLINE_TOPIC = "offline";
    private static final String AHENK_PREFIX = "ahenk-";


    private static volatile PulsarService pulsarService;
    private static volatile Producer<OnlineStatusMessageDTO> onlineProducer;
    private static volatile Producer<OnlineStatusMessageDTO> offlineProducer;

    private final ScheduledExecutorService healthCheckScheduler = Executors.newSingleThreadScheduledExecutor();

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
            log.error("Producer initialization failed for topic {}: {}", topicName, e.getMessage(), e);
            return null;
        }
    }

    private synchronized void initializeProducers() {
        if (onlineProducer == null) {
            onlineProducer = createProducer(ONLINE_TOPIC);
        }
        if (offlineProducer == null) {
            offlineProducer = createProducer(OFFLINE_TOPIC);
        }
    }

    private synchronized boolean isProducerHealthy(Producer<?> producer) {
        try {
            return producer != null && producer.isConnected();
        } catch (Exception e) {
            log.warn("Producer health check failed: {}", e.getMessage());
            return false;
        }
    }

    private void ensureProducersHealthy() {
        if (!isProducerHealthy(onlineProducer)) {
            log.info("Reinitializing online producer...");
            onlineProducer = createProducer(ONLINE_TOPIC);
        }
        if (!isProducerHealthy(offlineProducer)) {
            log.info("Reinitializing offline producer...");
            offlineProducer = createProducer(OFFLINE_TOPIC);
        }
    }

    private void sendMessage(Producer<OnlineStatusMessageDTO> producer, String topic, OnlineStatusMessageDTO message) {
        try {
            if (producer != null && producer.isConnected()) {
                if (message.getSubscriptionName().contains(AHENK_PREFIX)) {
                    producer.sendAsync(message);
                    log.info("Message sent to topic {}: {}", topic, message);
                } else {
                    log.info("System topic received {}", message);
                }

            } else {
                log.warn("Producer for topic {} is not connected. Message not sent: {}", topic, message);
                initializeProducers(); // Reinitialize producer if needed
            }
        } catch (Exception e) {
            log.error("Failed to send message to topic {}: {}", topic, e.getMessage(), e);
            initializeProducers(); // Attempt to reinitialize producer on failure
        }
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx) {
        log.info("New client connection established from IP: {}", cnx.clientAddress());
    }

    @Override
    public void consumerCreated(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
        log.info("Consumer connected with subscriptionName: {}", consumer.getSubscription().getName());

        if (onlineProducer == null || !onlineProducer.isConnected()) {
            initializeProducers();
        }

        OnlineStatusMessageDTO message = new OnlineStatusMessageDTO(consumer.getSubscription().getName());
        sendMessage(onlineProducer, ONLINE_TOPIC, message);
    }

    @Override
    public void consumerClosed(ServerCnx cnx, Consumer consumer, Map<String, String> metadata) {
        log.info("Consumer closed with subscriptionName: {}", consumer.getSubscription().getName());

        if (offlineProducer == null || !offlineProducer.isConnected()) {
            initializeProducers();
        }

        OnlineStatusMessageDTO message = new OnlineStatusMessageDTO(consumer.getSubscription().getName());
        sendMessage(offlineProducer, OFFLINE_TOPIC, message);
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
        // Implement if needed
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        log.info("Connection closed from IP: {}", cnx.clientAddress());
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
        initializeProducers();

        // Schedule health checks for producers
        healthCheckScheduler.scheduleAtFixedRate(this::ensureProducersHealthy, 1, 30, TimeUnit.SECONDS);
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
            healthCheckScheduler.shutdown();
            log.info("Health check scheduler stopped.");
        } catch (Exception e) {
            log.error("Failed to close producer: {}", e.getMessage(), e);
        }
    }
}
