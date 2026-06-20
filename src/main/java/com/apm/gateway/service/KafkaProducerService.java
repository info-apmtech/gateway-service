package com.apm.gateway.service;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class KafkaProducerService {

    private static final Logger LOG = Logger.getLogger(KafkaProducerService.class);

    @ConfigProperty(name = "kafka.bootstrap.servers", defaultValue = "localhost:9092")
    String bootstrapServers;

    private KafkaProducer<String, byte[]> binaryProducer;
    private KafkaProducer<String, String> stringProducer;

    void onStart(@Observes StartupEvent ev) {
        // Create binary producer for byte[] payloads
        Properties binaryProps = new Properties();
        binaryProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        binaryProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        binaryProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        binaryProps.put(ProducerConfig.ACKS_CONFIG, "1"); // Wait for leader acknowledgment
        binaryProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        binaryProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 16KB batch size
        binaryProps.put(ProducerConfig.LINGER_MS_CONFIG, 10); // Wait up to 10ms to batch
        binaryProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB buffer
        binaryProducer = new KafkaProducer<>(binaryProps);

        // Create string producer for String payloads
        Properties stringProps = new Properties();
        stringProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        stringProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        stringProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        stringProps.put(ProducerConfig.ACKS_CONFIG, "1");
        stringProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        stringProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        stringProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        stringProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        stringProducer = new KafkaProducer<>(stringProps);

        LOG.info("KafkaProducerService initialized with blocking KafkaProducer");
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (binaryProducer != null) {
            binaryProducer.flush();
            binaryProducer.close();
        }
        if (stringProducer != null) {
            stringProducer.flush();
            stringProducer.close();
        }
        LOG.info("KafkaProducerService shut down");
    }

    /**
     * Send binary data to Kafka (blocking call, safe to use in virtual threads)
     * With virtual threads, blocking I/O is efficient - the thread parks
     * instead of blocking OS thread
     */
    public void send(String topic, String key, byte[] payload) {
        try {
            // Normalize topic name
            String normalizedTopic = normalizeTopicName(topic);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(normalizedTopic, key, payload);

            // Blocking send - efficient in virtual threads (parks thread, doesn't block OS
            // thread)
            Future<RecordMetadata> future = binaryProducer.send(record);

            // Return immediately, but the send happens asynchronously by KafkaProducer
            // If you want to wait for completion, you can call future.get() here
            // With virtual threads, future.get() is efficient
            future.get();
        } catch (Exception ex) {
            LOG.errorf(ex, "[Kafka] ERROR sending binary to '%s': %s at %s",
                    topic, ex.getMessage(), Instant.now());
        }
    }

    /**
     * Send string data to Kafka (blocking call, safe to use in virtual threads)
     * With virtual threads, blocking I/O is efficient - the thread parks
     * instead of blocking OS thread
     */
    public void send(String topic, String key, String message) {
        try {
            // Normalize topic name
            String normalizedTopic = normalizeTopicName(topic);

            ProducerRecord<String, String> record = new ProducerRecord<>(normalizedTopic, key, message);

            // Blocking send - efficient in virtual threads (parks thread, doesn't block OS
            // thread)
            Future<RecordMetadata> future = stringProducer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    LOG.errorf(ex, "[Kafka] ERROR sending to '%s': %s imei: %s at %s",
                            topic, ex.getMessage(), key, Instant.now());
                }
            });

            // Return immediately, but the send happens asynchronously by KafkaProducer
            future.get();

        } catch (Exception ex) {
            LOG.errorf(ex, "[Kafka] ERROR sending to '%s': %s imei: %s at %s",
                    topic, ex.getMessage(), key, Instant.now());
        }
    }

    /**
     * Normalize topic names (handle both dot and hyphen variants)
     */
    private String normalizeTopicName(String topic) {
        // Map channel names to actual topic names using switch
        return switch (topic) {
            case "gps.raw.data", "gps-raw-data" ->
                "gps.raw.data";
            case "gps.raw.data.unknown", "gps-raw-data-unknown" ->
                "gps.raw.data.unknown";
            case "gps.command.response", "gps-command-response" ->
                "gps.command.response";
            case "gateway.status", "gateway-status" ->
                "gateway.status";
            default ->
                topic; // Return as-is if not recognized
        };
    }
}
