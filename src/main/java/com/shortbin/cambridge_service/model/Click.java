package com.shortbin.cambridge_service.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(keyspace = "shortbin", name = "clicks")
public class Click {

    @Column(name = "short_id")
    private String shortID;

    @Column(name = "timestamp")
    private java.util.Date timestamp;

    @Column(name = "id")
    private UUID id;

    @Column(name = "short_created_by")
    private Integer shortCreatedBy;

    @Column(name = "ip_address")
    private String ipAddress;

    @Column(name = "user_agent")
    private String userAgent;

    @Column(name = "referer")
    private String referer;

    // commenting for now, getting error
//    @Column(name = "x_forwarded_for")
//    private String xForwardedFor;

    @Column(name = "request_host")
    private String requestHost;
}
