package com.shortbin.cambridge_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickAggregate {
    private String shortId;
    private Integer count;
//    private Map<String, Integer> countryCounter;
//    private Map<String, Integer> referrerCounter;
//
//
//    public String getCountriesJson() {
//        ObjectMapper objectMapper = new ObjectMapper();
//        try {
//            return objectMapper.writeValueAsString(countryCounter);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//            return "{}";
//        }
//    }
//
//    public String getReferrersJson() {
//        ObjectMapper objectMapper = new ObjectMapper();
//        try {
//            return objectMapper.writeValueAsString(referrerCounter);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//            return "{}";
//        }
//    }

}