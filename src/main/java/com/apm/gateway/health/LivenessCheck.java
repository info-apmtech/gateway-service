package com.apm.gateway.health;

import com.apm.gateway.service.TcpListenerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@Liveness
@ApplicationScoped
public class LivenessCheck implements HealthCheck {

    @Inject
    TcpListenerService tcpListenerService;

    @Override
    public HealthCheckResponse call() {
        boolean live = tcpListenerService.isLive();
        return HealthCheckResponse.named("gateway-liveness")
                .status(live)
                .withData("status", live ? "running" : "stopped")
                .build();
    }
}
