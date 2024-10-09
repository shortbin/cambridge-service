package com.shortbin.cambridge_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickEvent {
    private String user_id;
    private Long page_id;
//    private String adId;
//    private String adType;
//    private String eventTime;
//    private String ipAddress;
//    private String browser;
//    private String os;
}
