import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import cv2
import numpy as np
import base64
import uuid
import logging
from datetime import datetime

# Import your verification functions
from utils.face_detection import FaceDetector, compare_faces_opencv
from utils.document_validation import validate_identity_document
from utils.role_document import verify_role_document

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Configure upload folder
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Helper functions
def save_uploaded_file(file, prefix="document"):
    """Save an uploaded file to disk and return the path"""
    try:
        filename = f"{prefix}_{uuid.uuid4().hex}_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg"
        save_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(save_path)
        return save_path
    except Exception as e:
        logging.error(f"Error saving file: {str(e)}")
        return None

def save_base64_image(base64_data, prefix="selfie"):
    """Save base64 image data to disk and return the path"""
    try:
        if "base64," in base64_data:
            base64_data = base64_data.split("base64,")[1]
        
        image_data = base64.b64decode(base64_data)
        filename = f"{prefix}_{uuid.uuid4().hex}_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg"
        save_path = os.path.join(UPLOAD_FOLDER, filename)
        
        with open(save_path, "wb") as f:
            f.write(image_data)
            
        return save_path
    except Exception as e:
        logging.error(f"Error saving base64 image: {str(e)}")
        return None

# API endpoints
@app.route('/api/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({"status": "ok", "message": "Verification API is running"})

@app.route('/api/verify/id', methods=['POST'])
def verify_id_document():
    """Verify national ID document"""
    try:
        if 'document' not in request.files:
            return jsonify({"valid": False, "error": "No document provided"}), 400
            
        file = request.files['document']
        
        # Save the uploaded file
        document_path = save_uploaded_file(file, prefix="id")
        if not document_path:
            return jsonify({"valid": False, "error": "Failed to save document"}), 500
            
        # Verify the ID document
        try:
            result = validate_identity_document(document_path)
            
            # Clean up file after processing
            if os.path.exists(document_path):
                os.remove(document_path)
                
            return jsonify(result)
            
        except Exception as e:
            logging.error(f"Error during document validation: {str(e)}")
            return jsonify({
                "valid": False, 
                "error": str(e),
                "reason": "Error validating document"
            }), 500
            
    except Exception as e:
        logging.error(f"Error in API endpoint: {str(e)}")
        return jsonify({"valid": False, "error": str(e)}), 500

@app.route('/api/verify/document/<role>', methods=['POST'])
def verify_role_document_api(role):
    """Verify role-specific document"""
    try:
        if 'document' not in request.files:
            return jsonify({"error": "No document provided"}), 400
            
        file = request.files['document']
        name = request.form.get('name', 'User')
        
        # Save the uploaded file
        document_path = save_uploaded_file(file, prefix=f"{role}_doc")
        if not document_path:
            return jsonify({"error": "Failed to save document"}), 500
            
        # Verify the role document
        result = verify_role_document(document_path, role, name)
        
        # Clean up file after processing
        if os.path.exists(document_path):
            os.remove(document_path)
            
        return jsonify(result)
        
    except Exception as e:
        logging.error(f"Error verifying {role} document: {str(e)}")
        return jsonify({"error": str(e), "verified": False}), 500

@app.route('/api/verify/face', methods=['POST'])
def verify_face():
    """Verify face against ID document"""
    try:
        if 'id_image' not in request.files:
            return jsonify({"error": "No ID image provided"}), 400
            
        id_file = request.files['id_image']
        
        if 'selfie_base64' not in request.form:
            return jsonify({"error": "No selfie image provided"}), 400
            
        selfie_base64 = request.form['selfie_base64']
        
        # Save the files
        id_path = save_uploaded_file(id_file, prefix="id_for_face")
        selfie_path = save_base64_image(selfie_base64, prefix="selfie")
        
        if not id_path or not selfie_path:
            return jsonify({"error": "Failed to save images"}), 500
            
        # Load the images
        id_image = cv2.imread(id_path)
        selfie_image = cv2.imread(selfie_path)
        
        if id_image is None or selfie_image is None:
            return jsonify({"error": "Failed to load images"}), 500
            
        # Convert BGR to RGB
        id_image_rgb = cv2.cvtColor(id_image, cv2.COLOR_BGR2RGB)
        selfie_image_rgb = cv2.cvtColor(selfie_image, cv2.COLOR_BGR2RGB)
        
        # Compare faces
        result = compare_faces_opencv(id_image_rgb, selfie_image_rgb)
        
        # Clean up files
        if os.path.exists(id_path):
            os.remove(id_path)
        if os.path.exists(selfie_path):
            os.remove(selfie_path)
            
        return jsonify(result)
        
    except Exception as e:
        logging.error(f"Error verifying face: {str(e)}")
        return jsonify({"error": str(e), "match": False, "confidence": 0}), 500

# At the bottom, update the run statement:
if __name__ == '__main__':
    # Use 0.0.0.0 to make the server accessible from any origin
    app.run(host='0.0.0.0', debug=True, port=8080)