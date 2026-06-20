package com.apm.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.RedisAPI;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
public class RedisClient {

    private static final Logger LOG = Logger.getLogger(RedisClient.class);

    @Inject
    Redis redis;

    @Inject
    ObjectMapper objectMapper;

    @ConfigProperty(name = "gateway.report-interval-seconds", defaultValue = "15")
    int reportIntervalSeconds;

    public void setGatewayImeisAsync(String nodeId, List<String> imeis) {
        try {
            RedisAPI api = RedisAPI.api(redis);
            String key = "gateway:" + nodeId + ":imeis";
            String value = objectMapper.writeValueAsString(imeis);

            // Set with expiration
            long expirationSeconds = reportIntervalSeconds + 5;
            api.setex(key, String.valueOf(expirationSeconds), value)
                    .subscribe().with(
                            result -> LOG.debugf("[Redis] Set IMEIs for node '%s'", nodeId),
                            failure -> {
                                LOG.errorf(failure, "[Redis] ERROR setting IMEIs for node '%s': %s at %s",
                                        nodeId, failure.getMessage(), Instant.now());
                            }
                    );
        } catch (Exception ex) {
            LOG.errorf(ex, "[Redis] ERROR setting IMEIs for node '%s': %s at %s",
                    nodeId, ex.getMessage(), Instant.now());
        }
    }
}
