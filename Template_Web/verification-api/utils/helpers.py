import cv2
import numpy as np
import logging
import os
import base64
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_image(image_path):
    """
    Load an image from file and convert to RGB
    
    Args:
        image_path: Path to the image file
    
    Returns:
        numpy.ndarray: RGB image or None if loading fails
    """
    try:
        img = cv2.imread(image_path)
        if img is None:
            logger.error(f"Failed to load image from {image_path}")
            return None
        
        # Convert from BGR to RGB
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        return img_rgb
    
    except Exception as e:
        logger.error(f"Error loading image: {str(e)}")
        return None

def preprocess_image(image, resize_dim=(800, 600)):
    """
    Preprocess an image for OCR and feature extraction
    
    Args:
        image: Input image as NumPy array
        resize_dim: Target dimensions for resizing
    
    Returns:
        tuple: (resized image, grayscale version, thresholded binary image)
    """
    try:
        if image is None:
            logger.warning("Null image provided to preprocess_image")
            return None, None, None
        
        # Make a copy to avoid modifying the original
        img = image.copy()
        
        # Resize for consistency
        img = cv2.resize(img, resize_dim)
        
        # Convert to grayscale
        if len(img.shape) == 3:
            gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
        else:
            gray = img.copy()
        
        # Enhance contrast
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced_gray = clahe.apply(gray)
        
        # Apply adaptive thresholding for text enhancement
        thresh = cv2.adaptiveThreshold(
            enhanced_gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 11, 2
        )
        
        return img, enhanced_gray, thresh
    
    except Exception as e:
        logger.error(f"Error preprocessing image: {str(e)}")
        return None, None, None

def save_base64_image(base64_data, prefix="doc"):
    """
    Save base64 encoded image to file.
    
    Args:
        base64_data: Base64 encoded image data
        prefix: Prefix for the saved filename
        
    Returns:
        str: Path to the saved file or None if failed
    """
    try:
        # Create uploads directory if it doesn't exist
        os.makedirs('uploaded_documents', exist_ok=True)
        
        # Handle data URL format
        if "base64," in base64_data:
            base64_data = base64_data.split("base64,")[1]
            
        # Decode base64 data
        image_data = base64.b64decode(base64_data)
        
        # Generate unique filename
        filename = f"{prefix}_{uuid.uuid4().hex}_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg"
        save_path = os.path.join('uploaded_documents', filename)
        
        # Save the file
        with open(save_path, 'wb') as f:
            f.write(image_data)
            
        logger.info(f"Saved base64 image: {save_path}")
        return save_path
    except Exception as e:
        logger.error(f"Error saving base64 image: {str(e)}")
        return None