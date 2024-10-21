package com.shortbin.cambridge_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickAggregate {
    private String shortId;
    private Integer count;
    private Map<String, Integer> countryCounter;
    private Map<String, Integer> refererCounter;
}