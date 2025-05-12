import cv2
import numpy as np
import logging
import os
import traceback
import re

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Try to import easyocr but provide a fallback if it's not available
try:
    import easyocr
    HAS_EASYOCR = True
    # Initialize reader only once (takes time to load models)
    reader = None
except ImportError:
    logger.warning("EasyOCR not installed. Using fallback detection methods.")
    HAS_EASYOCR = False
    reader = None

def get_ocr_reader():
    """Initialize and return the EasyOCR reader"""
    global reader
    if HAS_EASYOCR and reader is None:
        try:
            logger.info("Initializing EasyOCR reader...")
            reader = easyocr.Reader(['ar', 'en', 'fr'], gpu=False)
            logger.info("EasyOCR reader initialized")
        except Exception as e:
            logger.error(f"Error initializing EasyOCR: {e}")
            logger.error(traceback.format_exc())
            return None
    return reader

def extract_text_with_easyocr(image):
    """Extract text using EasyOCR if available"""
    if not HAS_EASYOCR:
        logger.warning("EasyOCR not available. Skipping text extraction.")
        return []
    
    try:
        reader = get_ocr_reader()
        if reader:
            results = reader.readtext(image)
            return results
        return []
    except Exception as e:
        logger.error(f"Error in OCR: {e}")
        logger.error(traceback.format_exc())
        return []

def validate_identity_document(image_path):
    """
    Simple ID document validation that doesn't rely solely on OCR
    """
    # Basic result structure
    result = {
        "valid": False,
        "confidence": 0.0,
        "detected_elements": [],
        "reason": "",
        "raw_text": ""
    }
    
    try:
        logger.debug(f"Starting validation for document: {image_path}")
        
        # Check if file exists and is readable
        if not os.path.exists(image_path):
            return {
                "valid": False,
                "confidence": 0.0,
                "reason": "File not found",
                "detected_elements": []
            }
        
        # Read image with OpenCV
        img = cv2.imread(image_path)
        if img is None:
            return {
                "valid": False,
                "confidence": 0.0,
                "reason": "Could not read image file",
                "detected_elements": []
            }
        
        # Save debug copy of original
        debug_dir = os.path.join(os.path.dirname(os.path.dirname(image_path)), "debug")
        os.makedirs(debug_dir, exist_ok=True)
        cv2.imwrite(os.path.join(debug_dir, "original.jpg"), img)
        
        # FALLBACK APPROACH: Instead of relying only on OCR, also analyze image properties
        height, width, channels = img.shape
        logger.debug(f"Image dimensions: {width}x{height}, channels: {channels}")
        
        # Calculate aspect ratio - many ID cards have a similar aspect ratio
        aspect_ratio = width / height
        logger.debug(f"Aspect ratio: {aspect_ratio:.2f}")
        
        # Convert to grayscale for processing
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Save debug grayscale
        cv2.imwrite(os.path.join(debug_dir, "grayscale.jpg"), gray)
        
        # Check edge density (IDs typically have a lot of edges from text and graphics)
        edges = cv2.Canny(gray, 100, 200)
        edge_count = np.sum(edges > 0)
        edge_density = edge_count / (width * height)
        logger.debug(f"Edge density: {edge_density:.4f}")
        
        # Save debug edges
        cv2.imwrite(os.path.join(debug_dir, "edges.jpg"), edges)
        
        # Check for face (most IDs have faces)
        face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
        faces = face_cascade.detectMultiScale(gray, 1.1, 4)
        has_face = len(faces) > 0
        logger.debug(f"Face detected: {has_face}")
        
        # Try OCR but don't rely on it exclusively
        ocr_results = extract_text_with_easyocr(img)
        all_text = ""
        if ocr_results:
            all_texts = [text.lower() for _, text, _ in ocr_results]
            all_text = " ".join(all_texts)
            result["raw_text"] = all_text
            logger.debug(f"OCR text: {all_text}")
        
        # Build confidence score based on multiple factors
        confidence = 0.0
        detected_elements = []
        
        # 1. Aspect ratio check (most ID cards are around 1.5:1 ratio)
        if 1.4 < aspect_ratio < 1.8:
            confidence += 0.2
            detected_elements.append("id_card_aspect_ratio")
        
        # 2. Edge density (IDs have significant detail)
        if edge_density > 0.05:  # Arbitrary threshold
            confidence += 0.2
            detected_elements.append("high_detail_density")
        
        # 3. Face detection
        if has_face:
            confidence += 0.3
            detected_elements.append("face_detected")
            
            # Draw rectangles around detected faces in debug image
            debug_img = img.copy()
            for (x, y, w, h) in faces:
                cv2.rectangle(debug_img, (x, y), (x+w, y+h), (255, 0, 0), 2)
            cv2.imwrite(os.path.join(debug_dir, "faces.jpg"), debug_img)
        
        # 4. OCR text patterns (if any text was detected)
        if all_text:
            # Check for digit sequences (common in IDs)
            digit_patterns = re.findall(r'\b\d{6,}\b', all_text)
            if digit_patterns:
                confidence += 0.2
                detected_elements.append(f"id_number_pattern: {digit_patterns[0]}")
            
            # Check for common ID text
            id_terms = ["identity", "carte", "id", "national", "passport", 
                        "هوية", "بطاقة", "وطنية", "تعريف"]
            for term in id_terms:
                if term in all_text:
                    confidence += 0.1
                    detected_elements.append(f"id_keyword: {term}")
                    break  # Only count one keyword match
        
        # Set final result
        result["confidence"] = min(confidence, 1.0)  # Cap at 1.0
        result["detected_elements"] = detected_elements
        
        # Even with no OCR text, we might have a valid ID
        # Use a lower threshold since we're using visual cues too
        if confidence >= 0.3:
            result["valid"] = True
            result["reason"] = f"ID verified with {confidence:.2f} confidence"
        else:
            result["valid"] = False
            if detected_elements:
                result["reason"] = f"Low confidence ({confidence:.2f}) for ID verification"
            else:
                result["reason"] = "No ID card elements detected"
        
        logger.debug(f"Final result: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error validating identity document: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "valid": False,
            "confidence": 0.0,
            "reason": f"Error during validation: {str(e)}",
            "detected_elements": [],
            "raw_text": ""
        }