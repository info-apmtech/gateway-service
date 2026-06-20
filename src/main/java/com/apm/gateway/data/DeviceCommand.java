package com.apm.gateway.data;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@Getter
@Setter
public class DeviceCommand {

    @JsonProperty("imei")
    private String imei;

    @JsonProperty("commandHex")
    private String commandHex;

    @JsonProperty("devicetype")
    private int devicetype;

    @JsonProperty("commandId")
    private int commandId;

    @JsonProperty("response")
    private String response;

    @JsonProperty("userName")
    private String userName;
}
