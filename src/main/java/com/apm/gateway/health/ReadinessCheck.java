package com.apm.gateway.health;

import com.apm.gateway.service.DeviceSessionManager;
import com.apm.gateway.service.TcpListenerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@Readiness
@ApplicationScoped
public class ReadinessCheck implements HealthCheck {

    @Inject
    TcpListenerService tcpListenerService;

    @Inject
    DeviceSessionManager sessionManager;

    @ConfigProperty(name = "gateway.port", defaultValue = "8225")
    int port;

    @Override
    public HealthCheckResponse call() {
        boolean ready = tcpListenerService.isReady();
        return HealthCheckResponse.named("gateway-readiness")
                .status(ready)
                .withData("tcpPort", port)
                .withData("tcpBound", ready)
                .withData("connectedDevices", sessionManager.getCount())
                .build();
    }
}
