package com.apm.gateway.service;

import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
public class SimpleKafkaBatcher {

    private static final Logger LOG = Logger.getLogger(SimpleKafkaBatcher.class);

    private final KafkaProducerService producer;
    private final String topic;
    private final List<BatchEntry> buffer = new ArrayList<>();
    private final Object lock = new Object();
    private final ScheduledExecutorService scheduler;
    private final int maxBatchSize;

    public SimpleKafkaBatcher(KafkaProducerService producer, String topic, int flushIntervalMs, int maxBatchSize) {
        this.producer = producer;
        this.topic = topic;
        this.maxBatchSize = maxBatchSize;
        this.scheduler = Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(this::flush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void add(String key, byte[] rawBytes) {
        boolean flushNow = false;
        synchronized (lock) {
            buffer.add(new BatchEntry(key, rawBytes));
            if (buffer.size() >= maxBatchSize) {
                flushNow = true;
            }
        }
        if (flushNow) {
            flush();
        }
    }

    public void flush() {
        List<BatchEntry> toSend;
        synchronized (lock) {
            if (buffer.isEmpty()) {
                return;
            }
            toSend = new ArrayList<>(buffer);
            buffer.clear();
        }

        for (BatchEntry entry : toSend) {
            producer.send(topic, entry.key, entry.hex);
        }
    }

    private static class BatchEntry {

        final String key;
        final byte[] hex;

        BatchEntry(String key, byte[] hex) {
            this.key = key;
            this.hex = hex;
        }
    }
}
