package com.apm.gateway.data;

import java.net.Socket;
import java.time.Instant;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@Getter
@Setter
public class DeviceSession {

    private Socket client;
    private String imei;
    private Instant lastSeen;
    private short lastSerial;

    public DeviceSession() {
        this.lastSeen = Instant.now();
    }

    public DeviceSession(Socket client) {
        this.client = client;
        this.lastSeen = Instant.now();
    }
}
