package com.shortbin.cambridge_service.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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


    public String getCountriesJson() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(countryCounter);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{}";
        }
    }

    public String getReferersJson() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(refererCounter);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{}";
        }
    }

}