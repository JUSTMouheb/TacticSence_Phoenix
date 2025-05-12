import requests
import json
import base64
import os
from pprint import pprint

# API base URL
BASE_URL = "http://localhost:5000/api"

def load_image_as_base64(image_path):
    """Convert an image file to base64 encoding."""
    try:
        with open(image_path, 'rb') as file:
            return base64.b64encode(file.read()).decode('utf-8')
    except Exception as e:
        print(f"Error loading image: {e}")
        return None

def test_health_check():
    """Test API health check endpoint."""
    print("\n=== Testing Health Check ===")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status: {response.status_code}")
        pprint(response.json())
        return response.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

# Update the test_identity_verification function to match your actual endpoint:

def test_identity_verification(id_image_path, selfie_image_path, name):
    """Test identity verification endpoint."""
    print("\n=== Testing Face Verification ===")
    
    # Create multipart/form-data request
    files = {
        'id_image': open(id_image_path, 'rb')
    }
    
    # Use base64 for selfie
    selfie_base64 = load_image_as_base64(selfie_image_path)
    
    data = {
        'selfie_base64': "data:image/jpeg;base64," + selfie_base64,
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/verify/face",
            files=files,
            data=data
        )
        
        print(f"Status: {response.status_code}")
        pprint(response.json())
        return response.status_code == 200
    except Exception as e:
        print(f"Face verification failed: {e}")
        return False

def test_document_verification(document_image_path, role, name):
    """Test role-specific document verification endpoint."""
    print(f"\n=== Testing {role} Document Verification ===")
    
    # Load image as base64
    doc_b64 = load_image_as_base64(document_image_path)
    
    if not doc_b64:
        print("Failed to load test image")
        return False
    
    # Prepare data
    data = {
        "document_image": "data:image/jpeg;base64," + doc_b64,
        "role": role,
        "name": name
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/verify/document",
            json=data,
            headers={"Content-Type": "application/json"}
        )
        
        print(f"Status: {response.status_code}")
        pprint(response.json())
        return response.status_code == 200
    except Exception as e:
        print(f"Document verification failed: {e}")
        return False

if __name__ == "__main__":
    # Set up test images - update these paths to point to your test images
    id_image = "test_images/id_sample.jpg"  # Path to a sample ID
    selfie_image = "test_images/selfie_sample.jpg"  # Path to a sample selfie 
    player_doc = "test_images/player_license.jpg"  # Path to a sample player license
    
    # Create test_images directory if it doesn't exist
    os.makedirs("test_images", exist_ok=True)
    
    # Check if test images exist
    if not all(os.path.exists(p) for p in [id_image, selfie_image, player_doc]):
        print("Warning: Some test images don't exist. Please add test images to the test_images directory.")
        print("You can continue with the tests that have available images.")
        
    # Run tests if the API is healthy
    if test_health_check():
        print("\nAPI is healthy, proceeding with verification tests")
        
        # Test identity verification if images exist
        if os.path.exists(id_image) and os.path.exists(selfie_image):
            test_identity_verification(id_image, selfie_image, "John Doe")
        else:
            print("Skipping identity verification test: missing test images")
            
        # Test document verification if image exists
        if os.path.exists(player_doc):
            test_document_verification(player_doc, "Player", "John Doe")
        else:
            print("Skipping document verification test: missing test image")
    else:
        print("\nAPI health check failed. Make sure the API is running.")