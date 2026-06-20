package com.apm.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.smallrye.common.annotation.RunOnVirtualThread;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class StatusReporterService {

    private static final Logger LOG = Logger.getLogger(StatusReporterService.class);

    @Inject
    DeviceSessionManager sessionManager;

    @Inject
    KafkaProducerService producer;

    @Inject
    ObjectMapper objectMapper;

    @ConfigProperty(name = "gateway.node-id", defaultValue = "unknown")
    String nodeId;

    @Scheduled(every = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @RunOnVirtualThread
    void reportStatus() {
        if (nodeId == null || nodeId.isEmpty()) {
            LOG.warn("[StatusReporter] Node ID is not configured.");
            nodeId = java.util.UUID.randomUUID().toString();
        }

        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("node", nodeId);
            payload.put("connected", sessionManager.getConnectedDeviceCount());
            payload.put("timestamp", Instant.now().toString());

            String json = objectMapper.writeValueAsString(payload);
            producer.send("gateway.status", nodeId, json);
        } catch (Exception ex) {
            LOG.errorf(ex, "[StatusReporter] ERROR: %s", ex.getMessage());
        }
    }
}
