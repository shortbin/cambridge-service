package com.shortbin.cambridge_service.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickEvent {

    @JsonProperty("short_id")
    private String shortID;

    @JsonProperty("short_created_by")
    private Integer shortCreatedBy;

    @JsonProperty("long_url")
    private String longUrl;

    @JsonProperty("ip_address")
    private String ipAddress;

    @JsonProperty("user_agent")
    private String userAgent;

    @JsonProperty("referer")
    private String referer;

    @JsonProperty("x_forwarded_for")
    private String xForwardedFor;

    @JsonProperty("request_host")
    private String requestHost;
}
