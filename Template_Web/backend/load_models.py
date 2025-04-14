import os
import joblib
import pandas as pd
import numpy as np
import warnings
from pprint import pprint
warnings.filterwarnings("ignore", category=UserWarning)

# Define model directory and file paths
MODELS_DIR = "models"
MODEL_PATH = os.path.join(MODELS_DIR, "fraud_detection_model.pkl")

class RobustFraudPredictor:
    def __init__(self):
        """Initialize the fraud predictor with direct feature extraction"""
        try:
            import sklearn
            print(f"Using scikit-learn version: {sklearn.__version__}")
            
            # Load the model
            self.model = joblib.load(MODEL_PATH)
            print(f"✅ Model successfully loaded from {MODEL_PATH}")
            
            # Extract feature names directly from the model
            self.feature_names = self._extract_feature_names()
            if self.feature_names:
                print(f"✅ Extracted {len(self.feature_names)} feature names directly from model")
                print("First 5 features:", self.feature_names[:5])
            else:
                print("❌ Could not extract feature names from model")
                exit(1)
                
        except Exception as e:
            print(f"❌ Error initializing predictor: {str(e)}")
            raise
    
    def _extract_feature_names(self):
        """Extract feature names directly from the model"""
        # Try different model attributes where feature names might be stored
        possible_attributes = [
            'feature_names_in_',  # Most common in sklearn 1.0+
            'feature_names',      # Some models use this
            'feature_name_',      # Older sklearn versions
            'n_features_in_'      # If we can't get names, maybe we can get count
        ]
        
        # Check if model has metadata
        if hasattr(self.model, 'get_booster') and hasattr(self.model.get_booster(), 'feature_names'):
            # XGBoost models
            return self.model.get_booster().feature_names
        
        # Check base estimators for ensemble models
        estimators_to_check = []
        
        # For ensemble models, check estimators array
        if hasattr(self.model, 'estimators_'):
            estimators_to_check.extend(self.model.estimators_)
        
        # For pipeline, check steps
        if hasattr(self.model, 'steps'):
            for _, estimator in self.model.steps:
                estimators_to_check.append(estimator)
                
        # For stacking models
        if hasattr(self.model, 'final_estimator_'):
            estimators_to_check.append(self.model.final_estimator_)
            
        # For meta estimators
        if hasattr(self.model, 'estimator'):
            estimators_to_check.append(self.model.estimator)
        
        # Check direct attributes
        for attr in possible_attributes:
            if hasattr(self.model, attr):
                feature_info = getattr(self.model, attr)
                if isinstance(feature_info, (list, np.ndarray)):
                    return list(feature_info)
                elif isinstance(feature_info, int):
                    # If we only have count, generate generic feature names
                    return [f'feature_{i}' for i in range(feature_info)]
        
        # Check all estimators
        for estimator in estimators_to_check:
            for attr in possible_attributes:
                if hasattr(estimator, attr):
                    feature_info = getattr(estimator, attr)
                    if isinstance(feature_info, (list, np.ndarray)):
                        return list(feature_info)
                    elif isinstance(feature_info, int):
                        return [f'feature_{i}' for i in range(feature_info)]
        
        # As a last resort, try to get from model dumps
        if hasattr(self.model, 'feature_importances_'):
            n_features = len(self.model.feature_importances_)
            return [f'feature_{i}' for i in range(n_features)]
        
        # Use manual extraction
        try:
            # Load some dummy data and see what sklearn complains about
            dummy_data = pd.DataFrame({f'feature_{i}': [0.5] for i in range(100)})
            try:
                self.model.predict(dummy_data)
            except ValueError as e:
                # Parse the error message to extract feature names
                err_msg = str(e)
                if "Feature names seen at fit time" in err_msg:
                    start = err_msg.find("[") + 1
                    end = err_msg.rfind("]")
                    if start > 0 and end > start:
                        features_str = err_msg[start:end]
                        return [f.strip(" '\"") for f in features_str.split(",")]
        except:
            pass
            
        return None
    
    def predict(self, input_data, auto_fill=True, verbose=False):
        """Make fraud prediction with auto-filling of missing features"""
        # Convert to DataFrame if it's a dict
        if isinstance(input_data, dict):
            input_df = pd.DataFrame([input_data])
        else:
            input_df = pd.DataFrame(input_data)
            
        if auto_fill:
            # Create a complete DataFrame with all required features
            complete_df = pd.DataFrame(index=input_df.index, columns=self.feature_names)
            
            # Fill in values we have from input
            for col in self.feature_names:
                if col in input_df.columns:
                    complete_df[col] = input_df[col]
                else:
                    # Auto-fill missing features with sensible defaults
                    if "score" in col or "ratio" in col or "credibility" in col:
                        complete_df[col] = 0.5
                    elif "risk" in col or "anomaly" in col:
                        complete_df[col] = 0.3
                    elif "count" in col or "days" in col:
                        complete_df[col] = 10
                    elif col == "active_in_transfer_window":
                        complete_df[col] = 0
                    else:
                        complete_df[col] = 0
                        
                    if verbose:
                        print(f"Auto-filling missing feature: {col}")
            
            input_df = complete_df
        else:
            # Validate all required features are present
            missing = set(self.feature_names) - set(input_df.columns)
            if missing:
                raise ValueError(f"Missing required features: {missing}")
            
            # Ensure columns are in the right order
            input_df = input_df[self.feature_names]
        
        # Make prediction
        try:
            prediction = self.model.predict(input_df)[0]
            probability = self.model.predict_proba(input_df)[0][1]
            
            # Get risk tier
            risk_tier = self._get_risk_tier(probability)
            
            return {
                'is_fraud': bool(prediction),
                'fraud_probability': round(probability * 100, 2),
                'risk_tier': risk_tier
            }
            
        except Exception as e:
            print(f"❌ Error making prediction: {str(e)}")
            return {
                'error': str(e),
                'fraud_probability': 50.0,
                'risk_tier': 'UNKNOWN'
            }
    
    def _get_risk_tier(self, probability):
        """Get risk tier based on fraud probability"""
        if probability > 0.8:
            return "VERY HIGH"
        elif probability > 0.6:
            return "HIGH"
        elif probability > 0.4:
            return "MEDIUM"
        elif probability > 0.2:
            return "LOW"
        else:
            return "VERY LOW"

# Example usage
if __name__ == "__main__":
    # Initialize the predictor
    predictor = RobustFraudPredictor()
    
    # Save the actual feature names for future reference
    if predictor.feature_names:
        actual_features_path = os.path.join(MODELS_DIR, "actual_features.pkl")
        joblib.dump(predictor.feature_names, actual_features_path)
        print(f"✅ Saved actual feature names to {actual_features_path}")
    
    print("\n----- TESTING PREDICTION -----")
    
    # Create test input with a few features
    test_input = {
        'experience_years': 6,
        'financial_risk_score': 0.6, 
        'network_risk_score': 0.3,
        'temporal_risk_score': 0.4,
        'data_quality_score': 0.85,
        'if_pred': 0,
        'lr_pred': 0,
        'ae_pred': 0
    }
    
    # Try prediction with auto-filling
    print("\nPrediction with auto-filling:")
    result = predictor.predict(test_input, auto_fill=True, verbose=True)
    print(f"🚨 Fraud: {'YES' if result.get('is_fraud', False) else 'NO'}")
    print(f"📊 Probability: {result.get('fraud_probability')}%")
    print(f"⚠️ Risk Tier: {result.get('risk_tier')}")
    
    # Show all feature names for reference
    print("\nAll feature names required by the model:")
    for i, feature in enumerate(predictor.feature_names):
        print(f"{i+1}. {feature}")