package com.tacticsense.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpEntity;
import java.util.Map;

// Example Spring Boot service to call the Python API

@Service
public class FraudDetectionService {
    
    private final RestTemplate restTemplate;
    private final String apiUrl = "http://localhost:5000/api/detect-fraud";
    
    public FraudDetectionService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }
    
    public FraudDetectionResult detectFraud(Map<String, Object> entityData) {
        try {
            // Call the Python API
            ResponseEntity<FraudDetectionResult> response = 
                restTemplate.postForEntity(apiUrl, entityData, FraudDetectionResult.class);
            
            return response.getBody();
        } catch (Exception e) {
            log.error("Error calling fraud detection API", e);
            
            // Return fallback result
            FraudDetectionResult fallback = new FraudDetectionResult();
            fallback.setFraudProbability(50.0);
            fallback.setRiskTier("ERROR");
            fallback.setRiskFactors(List.of("API connection error"));
            
            return fallback;
        }
    }
}