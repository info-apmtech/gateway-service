package com.apm.gateway.resource;

import com.apm.gateway.service.TcpListenerService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@Path("/health")
public class HealthResource {

    @Inject
    TcpListenerService tcpListenerService;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response health() {
        return readyResponse();
    }

    @GET
    @Path("/live")
    @Produces(MediaType.TEXT_PLAIN)
    public Response liveness() {
        return tcpListenerService.isLive()
                ? Response.ok("OK").build()
                : Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("NOT LIVE").build();
    }

    @GET
    @Path("/ready")
    @Produces(MediaType.TEXT_PLAIN)
    public Response readiness() {
        return readyResponse();
    }

    private Response readyResponse() {
        return tcpListenerService.isReady()
                ? Response.ok("OK").build()
                : Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("NOT READY").build();
    }
}
