import numpy as np
import random

class FraudDetectionService:
    """Service to make fraud predictions using loaded models"""
    
    def __init__(self):
        # Initialize your service here
        print("Fraud Detection Service initialized")
        # In a real implementation, you would load your models here
    
    def predict_fraud(self, data):
        """Make a fraud prediction for the given data"""
        # For now, return mock predictions
        # In a real implementation, you would use your loaded models
        entity_id = data.get('entityId', '')
        entity_type = data.get('entityType', '')
        
        # Generate a consistent but pseudo-random prediction based on the entity ID
        if entity_id:
            # Use the entity ID to generate a deterministic probability
            id_sum = sum(ord(c) for c in entity_id)
            probability = min(95, max(5, (id_sum % 100)))
        else:
            # Random probability between 5 and 95 percent
            probability = random.uniform(5, 95)
        
        # Determine risk tier based on probability
        if probability > 80:
            risk_tier = "VERY HIGH"
        elif probability > 60:
            risk_tier = "HIGH"
        elif probability > 40:
            risk_tier = "MEDIUM"
        elif probability > 20:
            risk_tier = "LOW"
        else:
            risk_tier = "VERY LOW"
        
        # Generate risk factors based on entity type and risk level
        risk_factors = self.generate_risk_factors(entity_type, probability)
        
        return {
            "fraudProbability": probability,
            "riskTier": risk_tier,
            "isFraud": probability > 70,
            "riskFactors": risk_factors
        }
    
    def generate_risk_factors(self, entity_type, probability):
        """Generate risk factors based on entity type and probability"""
        if probability < 30:
            return []
        
        # Number of risk factors based on probability
        num_factors = max(1, int(probability / 20))
        
        # Define type-specific risk factors
        risk_factors_map = {
            'players_agents': [
                "Inconsistent contract history",
                "Unusual pattern in client acquisition",
                "Financial transactions with high variance",
                "Low data consistency across records",
                "Missing verification documentation",
                "Suspicious international transfers"
            ],
            'recruiting_agents': [
                "Suspicious pattern in athlete placements",
                "Inconsistent track record documentation",
                "Verification needed for recent recruitments",
                "Unusual financial arrangements",
                "Network connections to flagged entities"
            ],
            'sporting_management_agencies': [
                "Unusual corporate structure with offshore entities",
                "Rapid expansion without corresponding resource increase",
                "Inconsistent client reporting practices",
                "Unusual commission structures compared to industry standards",
                "Multiple regulatory compliance flags in different jurisdictions"
            ],
            'communication_boxes': [
                "Publication of verifiably false information",
                "Coordinated media campaigns with betting market anomalies",
                "Financial ties to entities involved in previous fraud cases",
                "Systematic pattern of undisclosed paid content",
                "Manipulation of information timing for market advantage"
            ],
            'sponsors': [
                "Shell company structures used for sponsorship arrangements",
                "Significant discrepancy between reported and actual payments",
                "Unusual ROI claims without supporting methodology",
                "Pattern of sponsorship deals with related parties",
                "Financial flows through high-risk jurisdictions"
            ]
        }
        
        # Get risk factors for this type, or use generic ones
        all_factors = risk_factors_map.get(entity_type, [
            "Inconsistent documentation",
            "Unusual financial patterns",
            "Identity verification issues",
            "Network risk indicators",
            "Compliance concerns"
        ])
        
        # Randomly select factors, but ensure deterministic results for demo
        selected_indices = [(i + hash(entity_type)) % len(all_factors) for i in range(num_factors)]
        return [all_factors[i % len(all_factors)] for i in selected_indices]