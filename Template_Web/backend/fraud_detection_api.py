from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np  # Add this import
import logging
import os  # Add this for safer file path handling
from fraud_service import FraudDetectionService
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set Flask environment variables
os.environ['FLASK_ENV'] = 'development'
os.environ['FLASK_DEBUG'] = '1'

# Initialize the app and service
app = Flask(__name__)
CORS(app)  # Add CORS support to allow requests from Angular
service = FraudDetectionService()

# In your fraud_detection_api.py
@app.route('/api/entities/<stakeholder_type>', methods=['GET'])
def get_entities_by_type(stakeholder_type):
    """Get all entities for a specific stakeholder type"""
    try:
        # Define file path based on stakeholder type
        if stakeholder_type == 'players_agents':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Players_Agents_Dataset.csv')
            id_column = "license_number"
            name_column = "full_name"
        elif stakeholder_type == 'recruiting_agents':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Recruiting_Agents_Dataset.csv')
            id_column = "license_id"  # Might be different
            name_column = "agent_name"  # Might be different
        elif stakeholder_type == 'sporting_management_agencies':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Sporting_management_agencies_Dataset.csv')
            id_column = "license_number" 
            name_column = "agency_name"  # Likely different
        elif stakeholder_type == 'communication_boxes':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Communication_Boxes_Dataset.csv')
            id_column = "license_number"
            name_column = "company_name"  # Likely different
        elif stakeholder_type == 'sponsors':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Sponsors_Dataset.csv')
            id_column = "license_number"
            name_column = "sponsor_name"  # Likely different
        else:
            logger.warning(f"No dataset defined for entity type: {stakeholder_type}")
            return jsonify([])
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read CSV with {len(df)} rows for {stakeholder_type}")
        logger.info(f"CSV columns: {list(df.columns)}")
        
        # Try to find the correct ID and name columns if specified ones don't exist
        if id_column not in df.columns:
            possible_id_columns = [col for col in df.columns if 'id' in col.lower() or 'license' in col.lower() or 'number' in col.lower()]
            if possible_id_columns:
                id_column = possible_id_columns[0]
                logger.info(f"Using {id_column} as the ID column")
            else:
                # If no suitable ID column found, return error
                logger.error(f"No suitable ID column found in {stakeholder_type} dataset")
                return jsonify({"error": f"No ID column found in dataset for {stakeholder_type}"}), 400
                
        if name_column not in df.columns:
            possible_name_columns = [col for col in df.columns if 'name' in col.lower() or 'title' in col.lower()]
            if possible_name_columns:
                name_column = possible_name_columns[0]
                logger.info(f"Using {name_column} as the name column")
            else:
                # If no suitable name column found, return error
                logger.error(f"No suitable name column found in {stakeholder_type} dataset")
                return jsonify({"error": f"No name column found in dataset for {stakeholder_type}"}), 400
        
        # Create entities list
        entities = []
        for index, row in df.iterrows():
            try:
                entity = {
                    "id": str(row[id_column]),  # Convert to string 
                    "name": str(row[name_column]),
                    "type": stakeholder_type
                }
                entities.append(entity)
            except Exception as e:
                # Handle missing columns gracefully
                logger.error(f"Error processing row {index}: {str(e)}")
                continue
                
        logger.info(f"Returning {len(entities)} entities for {stakeholder_type}")
        return jsonify(entities)
            
    except Exception as e:
        logger.error(f"Error getting entities for {stakeholder_type}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500
@app.route('/api/debug/columns', methods=['GET'])
def debug_columns():
    """Show column names for all datasets"""
    try:
        results = {}
        
        for type_key, filename in [
            ('players_agents', 'Players_Agents_Dataset.csv'),
            ('recruiting_agents', 'Recruiting_Agents_Dataset.csv'),
            ('sporting_management_agencies', 'Sporting_management_agencies_Dataset.csv'),
            ('communication_boxes', 'Communication_Boxes_Dataset.csv'),
            ('sponsors', 'Sponsors_Dataset.csv')
        ]:
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', filename)
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    results[type_key] = {
                        "columns": list(df.columns)
                    }
                except Exception as e:
                    results[type_key] = {
                        "error": f"Error reading: {str(e)}"
                    }
            else:
                results[type_key] = {
                    "error": "File missing"
                }
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# Add more detailed logging
@app.route('/api/detect-fraud', methods=['POST'])
def detect_fraud():
    """Endpoint for fraud detection"""
    try:
        # Get data from either JSON or form
        data = request.json if request.is_json else request.form.to_dict()
        
        # Log the raw request data for debugging
        logger.info(f"Raw request data: {data}")
        
        entity_id = data.get('entityId')
        entity_type = data.get('entityType')
        
        logger.info(f"Processing fraud detection for {entity_type} with ID {entity_id}")
        
        if not entity_id or not entity_type:
            logger.warning("Missing required parameters")
            return jsonify({"error": "Missing required parameters"}), 400
        
        # Get entity details from the dataset
        entity_details = get_entity_details(entity_id, entity_type)
        
        if not entity_details:
            logger.error(f"Entity with ID {entity_id} not found in {entity_type} dataset")
            # Return a more helpful error with sample IDs from the dataset
            return jsonify({
                "error": f"Entity with ID {entity_id} not found", 
                "status": "error"
            }), 404
        
        # Call the fraud service to analyze the entity
        result = service.predict_fraud(data)
        
        # Add entity details to the result
        result["entityDetails"] = entity_details
        
        # Log the full result for debugging
        logger.info(f"Sending response with entity details: {entity_details.get('full_name', 'Unknown')}")
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error in fraud detection: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e), "status": "error"}), 500

@app.route('/api/debug/csv/<stakeholder_type>', methods=['GET'])
def debug_csv(stakeholder_type):
    """Debug endpoint to check CSV contents"""
    try:
        # Fix the env check - app.env doesn't exist in newer Flask versions
        if not app.debug:  # Use app.debug instead of app.env
            return jsonify({"error": "Not available in production"}), 403
            
        # Define file path based on entity type
        if stakeholder_type == 'players_agents':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Players_Agents_Dataset.csv')
        elif stakeholder_type == 'recruiting_agents':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Recruiting_Agents_Dataset.csv')
        elif stakeholder_type == 'sporting_management_agencies' or stakeholder_type == 'Sporting_management_agencies':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Sporting_management_agencies_Dataset.csv')
        elif stakeholder_type == 'communication_boxes':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Communication_Boxes_Dataset.csv')
        elif stakeholder_type == 'sponsors':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Sponsors_Dataset.csv')
        else:
            return jsonify({"error": "Invalid stakeholder type"}), 400
            
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404
            
        # Read the CSV and return first 5 rows as JSON
        df = pd.read_csv(file_path)
        sample = df.head().to_dict(orient='records')
        return jsonify({
            "total_rows": len(df),
            "columns": list(df.columns),
            "sample": sample
        })
        
    except Exception as e:
        logger.error(f"Error in debug route: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

@app.route('/api/debug/entity-counts', methods=['GET'])
def debug_entity_counts():
    """Show counts of entities for all stakeholder types"""
    try:
        results = {}
        
        for type_key, filename in [
            ('players_agents', 'Players_Agents_Dataset.csv'),
            ('recruiting_agents', 'Recruiting_Agents_Dataset.csv'),
            ('sporting_management_agencies', 'Sporting_management_agencies_Dataset.csv'),
            ('communication_boxes', 'Communication_Boxes_Dataset.csv'),
            ('sponsors', 'Sponsors_Dataset.csv')
        ]:
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', filename)
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    entity_count = len(df)
                    file_status = "Found"
                except Exception as e:
                    entity_count = 0
                    file_status = f"Error reading: {str(e)}"
            else:
                entity_count = 0
                file_status = "File missing"
            
            results[type_key] = {
                "file_status": file_status,
                "entity_count": entity_count,
                "file_path": file_path
            }
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_entity_details(entity_id, entity_type):
    """Get detailed entity information from the appropriate dataset"""
    try:
        # Define file path and column names based on entity type
        if entity_type == 'players_agents':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Players_Agents_Dataset.csv')
            id_column = "license_number"
            name_column = "full_name"
        elif entity_type == 'recruiting_agents':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Recruiting_Agents_Dataset.csv')
            id_column = "license_id"  # Use the same column as in get_entities_by_type
            name_column = "agent_name"
        elif entity_type == 'sporting_management_agencies' or entity_type == 'Sporting_management_agencies':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Sporting_management_agencies_Dataset.csv')
            id_column = "license_number"
            name_column = "agency_name"
        elif entity_type == 'communication_boxes':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Communication_Boxes_Dataset.csv')
            id_column = "license_number"
            name_column = "company_name"
        elif entity_type == 'sponsors':
            file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', 'Sponsors_Dataset.csv')
            id_column = "license_number"
            name_column = "sponsor_name"
        else:
            logger.warning(f"No dataset defined for entity type: {entity_type}")
            return None
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"CSV file not found: {file_path}")
            return None
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read CSV with {len(df)} rows")
        
        # Debug: Log the columns to verify what's available
        logger.info(f"CSV columns for {entity_type}: {list(df.columns)}")
        
        # Use dynamic column detection like in get_entities_by_type
        if id_column not in df.columns:
            possible_id_columns = [col for col in df.columns if 'id' in col.lower() or 'license' in col.lower() or 'number' in col.lower()]
            if possible_id_columns:
                id_column = possible_id_columns[0]
                logger.info(f"Using {id_column} as the ID column for entity details")
        
        # Debug: Log sample IDs with the correct column
        sample_ids = df[id_column].head().tolist() if id_column in df.columns else []
        logger.info(f"Sample IDs in CSV using column {id_column}: {sample_ids}")
        logger.info(f"Looking for ID: '{entity_id}' (type: {type(entity_id)})")
        
        # Find the entity by ID with the correct column name
        if id_column in df.columns:
            entity_row = df[df[id_column] == entity_id]
            
            # Try different type conversions if not found
            if entity_row.empty:
                logger.warning(f"Entity with ID {entity_id} not found in column {id_column}")
                
                # Try as string if it's not already a string
                if not isinstance(entity_id, str):
                    logger.info(f"Trying to find ID as string: '{str(entity_id)}'")
                    entity_row = df[df[id_column] == str(entity_id)]
                
                # Try as integer if it's currently a string
                elif isinstance(entity_id, str):
                    try:
                        int_id = int(entity_id)
                        logger.info(f"Trying to find ID as integer: {int_id}")
                        entity_row = df[df[id_column] == int_id]
                    except ValueError:
                        pass
                
                # If still not found, try case-insensitive comparison
                if entity_row.empty and isinstance(entity_id, str) and df[id_column].dtype == object:
                    logger.info("Trying case-insensitive ID match")
                    matches = df[df[id_column].str.lower() == entity_id.lower()]
                    if not matches.empty:
                        entity_row = matches
        else:
            logger.error(f"ID column '{id_column}' not found in CSV columns")
            return None
        
        if entity_row.empty:
            logger.error(f"Entity with ID {entity_id} not found after all attempts")
            return None
            
        # Convert row to dictionary
        entity_data = entity_row.iloc[0].to_dict()
        logger.info(f"Found entity: {entity_data.get(name_column, 'Unknown')}")
        
        # Convert numpy types to native Python types for JSON serialization
        for key, value in entity_data.items():
            if isinstance(value, (np.int64, np.int32)):
                entity_data[key] = int(value)
            elif isinstance(value, (np.float64, np.float32)):
                entity_data[key] = float(value)
            elif pd.isna(value):
                entity_data[key] = None
        
        return entity_data
        
    except Exception as e:
        logger.error(f"Error getting entity details: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok", "service": "Fraud Detection API"})

if __name__ == '__main__':
    # Test that all dataset files exist
    datasets = [
        ('players_agents', 'Players_Agents_Dataset.csv'),
        ('recruiting_agents', 'Recruiting_Agents_Dataset.csv'),
        ('sporting_management_agencies', 'Sporting_management_agencies_Dataset.csv'),
        ('communication_boxes', 'Communication_Boxes_Dataset.csv'),
        ('sponsors', 'Sponsors_Dataset.csv')
    ]
    
    for type_key, filename in datasets:
        file_path = os.path.join('c:', os.sep, 'Modeling Notebooks', filename)
        if os.path.exists(file_path):
            logger.info(f"✓ Found dataset for {type_key}: {file_path}")
        else:
            logger.warning(f"✗ Missing dataset for {type_key}: {file_path}")
    
    logger.info("Starting Fraud Detection API on port 5001")
    app.run(host='0.0.0.0', port=5001, debug=True)