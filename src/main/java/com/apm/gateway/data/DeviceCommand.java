package com.apm.gateway.data;

import com.fasterxml.jackson.annotation.JsonAlias;
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
    @JsonAlias("Imei")
    private String imei;

    @JsonProperty("commandHex")
    @JsonAlias("CommandHex")
    private String commandHex;

    @JsonProperty("devicetype")
    @JsonAlias("DeviceType")
    private int devicetype;

    @JsonProperty("commandId")
    @JsonAlias("CommandId")
    private int commandId;

    @JsonProperty("response")
    @JsonAlias("Response")
    private String response;

    @JsonProperty("userName")
    @JsonAlias("UserName")
    private String userName;
}
