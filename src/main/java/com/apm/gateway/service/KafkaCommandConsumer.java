package com.apm.gateway.service;

import com.apm.gateway.data.DeviceCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.smallrye.common.annotation.RunOnVirtualThread;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class KafkaCommandConsumer {

    private static final Logger LOG = Logger.getLogger(KafkaCommandConsumer.class);

    @Inject
    DeviceSessionManager sessionManager;

    @Inject
    KafkaProducerService producer;

    @Inject
    ObjectMapper objectMapper;

    @ConfigProperty(name = "kafka.command-topic", defaultValue = "gps.command.to.device")
    String commandTopic;

    @ConfigProperty(name = "kafka.command-response-topic", defaultValue = "gps.command.response")
    String commandResponseTopic;

    @Incoming("gps-command-to-device")
    @RunOnVirtualThread
    public void consume(String message) {
        LOG.info("KafkaCommandConsumer started on virtual thread " + Thread.currentThread().isVirtual());
        try {
            DeviceCommand cmd = objectMapper.readValue(message, DeviceCommand.class);
            if (cmd == null || cmd.getImei() == null || cmd.getImei().isEmpty()
                    || cmd.getCommandHex() == null || cmd.getCommandHex().isEmpty()) {
                LOG.warn("[CommandConsumer] Invalid command received.");
                return;
            }

            DeviceCommand status = sessionManager.sendCommandAndReadResponse(cmd);
            if (status != null) {
                byte[] statusBytes = objectMapper.writeValueAsBytes(status);
                producer.send(commandResponseTopic, cmd.getImei(), statusBytes);
            }

            LOG.infof("[CommandConsumer] Command %s to %s", status, cmd.getImei());
        } catch (Exception ex) {
            LOG.errorf(ex, "[KafkaCommandConsumer] ERROR processing command: %s", ex.getMessage());
        }
    }
}
