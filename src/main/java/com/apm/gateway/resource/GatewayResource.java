package com.apm.gateway.resource;

import com.apm.gateway.service.DeviceSessionManager;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@Path("/api")
public class GatewayResource {

    @Inject
    DeviceSessionManager sessionManager;

    @ConfigProperty(name = "app.version", defaultValue = "unknown")
    String appVersion;

    @ConfigProperty(name = "api.key")
    String apiKey;

    @GET
    @Path("/version")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVersion() {
        return Response.ok(Map.of("version", appVersion)).build();
    }

    @GET
    @Path("/devices/count")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDeviceCount() {
        int count = sessionManager.getConnectedDeviceCount();
        return Response.ok(Map.of("count", count)).build();
    }

    @GET
    @Path("/devices/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDeviceList() {
        List<String> imeis = sessionManager.getConnectedImeis();
        return Response.ok(imeis).build();
    }
}
