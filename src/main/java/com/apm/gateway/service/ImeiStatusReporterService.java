package com.apm.gateway.service;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.smallrye.common.annotation.RunOnVirtualThread;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class ImeiStatusReporterService {

    private static final Logger LOG = Logger.getLogger(ImeiStatusReporterService.class);

    @Inject
    DeviceSessionManager sessions;

    @Inject
    RedisClient redis;

    @ConfigProperty(name = "gateway.node-id", defaultValue = "unknown")
    String nodeId;

    private volatile Set<String> lastImeis = new HashSet<>();

    @Scheduled(every = "${gateway.report-interval-seconds}s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @RunOnVirtualThread
    void reportImeis() {
        try {
            List<String> currentImeis = sessions.getConnectedImeis();
            Set<String> currentImeisSet = new HashSet<>(currentImeis);

            if (!lastImeis.equals(currentImeisSet)) {
                lastImeis = currentImeisSet;
                redis.setGatewayImeisAsync(nodeId, currentImeis);
                LOG.infof("IMEIs updated in Redis: %d", currentImeis.size());
            } else {
                LOG.debug("IMEIs unchanged, skipping Redis update.");
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "Error in IMEI status reporting");
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOG.info("ImeiStatusReporterService stopping, updating Redis to 'stopped'");
        try {
            redis.setGatewayImeisAsync(nodeId, List.of("stopped"));
        } catch (Exception ex) {
            LOG.errorf(ex, "Error setting stopped state to Redis");
        }
    }
}
