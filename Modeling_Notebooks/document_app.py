import streamlit as st
import os
import cv2
import numpy as np
import easyocr
import matplotlib.pyplot as plt
import uuid
import re
import base64
from datetime import datetime
import logging
from PIL import Image
import io
from unidecode import unidecode
import warnings
warnings.filterwarnings('ignore')

# Try to import SSIM with a fallback option
try:
    from skimage.metrics import structural_similarity as ssim
except ImportError:
    def ssim(img1, img2, **kwargs):
        """Simple fallback when skimage is not available"""
        try:
            if img1.shape != img2.shape:
                img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
            
            mse = np.mean((img1.astype(float) - img2.astype(float)) ** 2)
            if mse == 0:
                return 1.0
            max_err = 255.0 ** 2
            return 1.0 - min(mse / max_err, 1.0)
        except Exception as e:
            st.error(f"SSIM calculation error: {e}")
            return 0.3  # Default medium similarity

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create necessary directories
os.makedirs('uploaded_documents', exist_ok=True)
os.makedirs('processed_results', exist_ok=True)
os.makedirs('./uploads', exist_ok=True)

# Initialize EasyOCR reader
@st.cache_resource
def load_easyocr_reader():
    try:
        st.info("Loading EasyOCR model (this may take a moment)...")
        # Arabic is compatible with these languages only
        return easyocr.Reader(['ar', 'en'], gpu=False)
    except Exception as e:
        st.error(f"Error loading EasyOCR: {str(e)}")
        return None

# Image Processing Helper Functions
def load_image(image_file):
    if image_file is None:
        return None
        
    try:
        if isinstance(image_file, str):  # File path
            img = cv2.imread(image_file)
            if img is None:
                raise Exception(f"Failed to load image from {image_file}")
            img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            return img_rgb
        else:  # Streamlit uploaded file
            image_bytes = image_file.getvalue()
            nparr = np.frombuffer(image_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    except Exception as e:
        st.error(f"Error loading image: {str(e)}")
        return None

def preprocess_image(image, resize_dim=(800, 600)):
    try:
        if image is None:
            return None, None, None
        img = image.copy()
        img = cv2.resize(img, resize_dim)
        gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
        
        # Enhance contrast
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced_gray = clahe.apply(gray)
        
        thresh = cv2.adaptiveThreshold(enhanced_gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                      cv2.THRESH_BINARY, 11, 2)
        return img, enhanced_gray, thresh
    except Exception as e:
        st.error(f"Error preprocessing image: {str(e)}")
        return None, None, None

def save_uploaded_file(uploaded_file, prefix="document"):
    if uploaded_file is None:
        return None
    try:
        file_ext = os.path.splitext(uploaded_file.name)[1]
        filename = f"{prefix}_{uuid.uuid4().hex}_{datetime.now().strftime('%Y%m%d%H%M%S')}{file_ext}"
        save_path = os.path.join('uploaded_documents', filename)
        
        with open(save_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())
            
        logger.info(f"Saved uploaded file: {save_path}")
        return save_path
    except Exception as e:
        logger.error(f"Error saving uploaded file: {str(e)}")
        return None

def extract_text_with_easyocr(image):
    try:
        if image is None:
            return []
            
        reader = load_easyocr_reader()
        if reader is None:
            st.error("OCR model failed to load")
            return []
            
        # Create multiple versions of the image for better text detection
        results = []
        
        # First try with enhanced contrast
        enhanced = image.copy()
        if len(enhanced.shape) == 3:
            enhanced = cv2.convertScaleAbs(enhanced, alpha=1.2, beta=10)
        results.extend(reader.readtext(enhanced, detail=1, paragraph=False))
        
        # Try with gray version
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
            gray = cv2.equalizeHist(gray)  # Improve contrast
            gray_results = reader.readtext(gray, detail=1, paragraph=False)
            for r in gray_results:
                if r not in results:
                    results.append(r)
                    
        return results
    except Exception as e:
        st.error(f"Error in OCR processing: {str(e)}")
        return []

def visualize_ocr_results(image, ocr_results):
    if image is None or not ocr_results:
        return None
    output = image.copy()
    for (bbox, text, prob) in ocr_results:
        (tl, tr, br, bl) = bbox
        tl = (int(tl[0]), int(tl[1]))
        tr = (int(tr[0]), int(tr[1]))
        br = (int(br[0]), int(br[1]))
        bl = (int(bl[0]), int(bl[1]))
        cv2.polylines(output, [np.array([tl, tr, br, bl])], True, (0, 255, 0), 2)
        cv2.putText(output, text, (tl[0], tl[1] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
    return output

def normalize_name(name):
    if not name:
        return ""
    is_arabic = any('\u0600' <= c <= '\u06FF' for c in name)
    if is_arabic:
        normalized = ''.join(c for c in name if not '\u064B' <= c <= '\u0652')
        normalized = normalized.replace('\u0640', '')
        normalized = normalized.lower()
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized.strip()
    else:
        name = name.lower()
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'[^\w\s]', '', name)
        replacements = {
            'mohamed': 'mohammed',
            'muhammed': 'mohammed',
            'mohamad': 'mohammed',
            'abd': 'abdul',
            'ben': 'bin'
        }
        words = name.split()
        normalized_words = []
        for word in words:
            if word in replacements:
                normalized_words.append(replacements[word])
            else:
                normalized_words.append(word)
        return ' '.join(normalized_words).strip()

# Added missing compare_names function
def compare_names(name1, name2):
    if not name1 or not name2:
        return {"match": False, "similarity": 0.0}
        
    is_arabic1 = any('\u0600' <= c <= '\u06FF' for c in name1)
    is_arabic2 = any('\u0600' <= c <= '\u06FF' for c in name2)
    
    # Handle mixed language comparisons
    if is_arabic1 != is_arabic2:
        norm1 = normalize_name(name1)
        norm2 = normalize_name(name2)
        
        # Simple substring match for mixed language
        shorter = norm1 if len(norm1) < len(norm2) else norm2
        longer = norm2 if len(norm1) < len(norm2) else norm1
        
        if shorter in longer:
            similarity = len(shorter) / len(longer)
            return {"match": similarity > 0.5, "similarity": similarity}
            
        # Try word-level comparison for mixed languages
        words1 = set(norm1.split())
        words2 = set(norm2.split())
        intersection = len(words1.intersection(words2))
        
        if intersection > 0:
            similarity = intersection / max(len(words1), len(words2))
            return {"match": similarity > 0.3, "similarity": similarity}
            
        # Default low similarity for mixed languages with no match
        return {"match": False, "similarity": 0.1}
    
    # Same language comparison
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)
    
    # Exact match
    if norm1 == norm2:
        return {"match": True, "similarity": 1.0}
        
    # Word-level comparison
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    # Different thresholds based on language
    threshold = 0.5 if is_arabic1 else 0.7
    
    # Check if one is subset of the other
    if words1.issubset(words2) or words2.issubset(words1):
        common_words = words1.intersection(words2)
        total_words = max(len(words1), len(words2))
        if total_words > 0:
            similarity = len(common_words) / total_words
            return {"match": similarity > threshold, "similarity": similarity}
            
    # Handle empty sets
    if not words1 or not words2:
        return {"match": False, "similarity": 0.0}
        
    # Jaccard similarity for partial matches
    intersection = len(words1.intersection(words2))
    union = len(words1.union(words2))
    similarity = intersection / union if union > 0 else 0.0
    
    return {"match": similarity > threshold, "similarity": similarity}

def compare_faces(face1, face2):
    if face1 is None or face2 is None:
        return 0.0
    
    try:
        # Ensure both faces are the same size
        if face1.shape != face2.shape:
            face2 = cv2.resize(face2, (face1.shape[1], face1.shape[0]))
            
        # Convert to grayscale
        if len(face1.shape) == 3:
            face1_gray = cv2.cvtColor(face1, cv2.COLOR_RGB2GRAY)
        else:
            face1_gray = face1
            
        if len(face2.shape) == 3:
            face2_gray = cv2.cvtColor(face2, cv2.COLOR_RGB2GRAY)
        else:
            face2_gray = face2
        
        # Apply preprocessing for better comparison
        face1_gray = cv2.equalizeHist(face1_gray)
        face2_gray = cv2.equalizeHist(face2_gray)
        
        face1_gray = cv2.GaussianBlur(face1_gray, (5, 5), 0)
        face2_gray = cv2.GaussianBlur(face2_gray, (5, 5), 0)
        
        # Try different comparison methods and take the best result
        methods = []
        
        # Method 1: Structural similarity (SSIM)
        try:
            from skimage.metrics import structural_similarity as ssim
            score1 = ssim(face1_gray, face2_gray)
            # Use raw SSIM score without artificial boosting
            methods.append(score1)
        except Exception:
            pass
            
        # Method 2: Mean squared error with more realistic threshold
        mse = np.mean((face1_gray.astype("float") - face2_gray.astype("float")) ** 2)
        methods.append(max(0, 1 - mse / 8000))  # More realistic threshold
        
        # Method 3: Histogram comparison
        hist1 = cv2.calcHist([face1_gray], [0], None, [256], [0, 256])
        hist2 = cv2.calcHist([face2_gray], [0], None, [256], [0, 256])
        hist1 = cv2.normalize(hist1, hist1).flatten()
        hist2 = cv2.normalize(hist2, hist2).flatten()
        correlation = cv2.compareHist(hist1, hist2, cv2.HISTCMP_CORREL)
        methods.append(correlation)
        
        # Return the maximum similarity from all methods
        # Remove artificial minimum score - report the actual similarity
        return max(methods)
        
    except Exception as e:
        st.error(f"Face comparison error: {str(e)}")
        return 0.0  # Return 0 for error cases

# Enhanced Face Detection Functions
def detect_face(image):
    if image is None:
        return None
    
    # Try multiple approaches for face detection
    try:
        # Make a copy to prevent modification
        img = image.copy()
        
        # First approach: Standard OpenCV cascade classifier
        gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
        gray = cv2.equalizeHist(gray)  # Enhance contrast
        
        # Try multiple cascades with different parameters
        faces = []
        face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
        
        # Try with decreasing strictness
        cascades = [
            (face_cascade, 1.1, 5, (30, 30)),
            (face_cascade, 1.2, 4, (25, 25)),
            (face_cascade, 1.3, 3, (20, 20)),
            (face_cascade, 1.5, 2, (10, 10)),
            (cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_alt.xml'), 
             1.1, 3, (20, 20)),
            (cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_alt2.xml'), 
             1.1, 2, (15, 15))
        ]
        
        for cascade, scale, neighbors, min_size in cascades:
            faces = cascade.detectMultiScale(
                gray, 
                scaleFactor=scale, 
                minNeighbors=neighbors, 
                minSize=min_size
            )
            if len(faces) > 0:
                break
        
        # If face detected, extract and return it
        if len(faces) > 0:
            x, y, w, h = faces[0]
            # Add padding around the face
            padding = int(w * 0.2)  # Increased padding to 20%
            x = max(0, x - padding)
            y = max(0, y - padding)
            w = min(image.shape[1] - x, w + 2*padding)
            h = min(image.shape[0] - y, h + 2*padding)
            face_img = img[y:y+h, x:x+w]
            return cv2.resize(face_img, (120, 120))  # Increased resolution
        
        # If no face detected, use center crop as fallback
        h, w = img.shape[:2]
        center_x, center_y = w // 2, h // 2
        # Use a larger crop size for better results
        crop_size = min(w, h) // 2  
        x1 = max(0, center_x - crop_size)
        y1 = max(0, center_y - crop_size)
        x2 = min(w, center_x + crop_size)
        y2 = min(h, center_y + crop_size)
        
        center_crop = img[y1:y2, x1:x2]
        return cv2.resize(center_crop, (120, 120))
    
    except Exception as e:
        st.error(f"Face detection error: {str(e)}")
        # Emergency fallback - just resize the whole image
        return cv2.resize(image, (120, 120))

def verify_identity(id_img, face_img, role_img, user_name, role_type):
    """Complete identity verification function with both documents"""
    result = {
        "overall_verification": {"verified": False},
        "id_name_match": False,
        "role_doc_match": False,
        "face_similarity": 0,
        "confidence": 0
    }
    
    try:
        if id_img is None or face_img is None:
            st.error("Error: Failed to load one or both images")
            return result
            
        # Extract text from ID document
        st.subheader("ID Document Analysis")
        id_ocr_results = extract_text_with_easyocr(id_img)
        st.info(f"Detected text blocks in ID: {len(id_ocr_results)}")
        
        # Display extracted text blocks from ID
        for i, (_, text, prob) in enumerate(id_ocr_results):
            st.write(f"  Text {i}: {text} (Confidence: {prob:.2f})")
            
        # Find name match in ID document
        best_match_id = None
        best_similarity_id = 0
        
        for _, text, _ in id_ocr_results:
            comparison = compare_names(text, user_name)
            if comparison["similarity"] > best_similarity_id:
                best_similarity_id = comparison["similarity"]
                best_match_id = text
                
                if comparison["similarity"] > 0.5:
                    result["id_name_match"] = True
                    st.success(f"Extracted name from ID: {text}")
                    st.success(f"Name comparison: {user_name} vs {text} = {comparison['similarity']:.2f}")
        
        # Extract text from role document if provided
        result["role_doc_match"] = False
        if role_img is not None:
            st.subheader(f"{role_type} Document Analysis")
            role_ocr_results = extract_text_with_easyocr(role_img)
            st.info(f"Detected text blocks in {role_type} document: {len(role_ocr_results)}")
            
            # Display extracted text blocks from role document
            for i, (_, text, prob) in enumerate(role_ocr_results):
                st.write(f"  Text {i}: {text} (Confidence: {prob:.2f})")
            
            # Find name match in role document
            best_match_role = None
            best_similarity_role = 0
            
            for _, text, _ in role_ocr_results:
                comparison = compare_names(text, user_name)
                if comparison["similarity"] > best_similarity_role:
                    best_similarity_role = comparison["similarity"]
                    best_match_role = text
                    
                    if comparison["similarity"] > 0.5:
                        result["role_doc_match"] = True
                        st.success(f"Extracted name from {role_type} document: {text}")
                        st.success(f"Name comparison: {user_name} vs {text} = {comparison['similarity']:.2f}")
        
        # Face detection and comparison
        id_face = detect_face(id_img)
        webcam_face = detect_face(face_img)
        
        # Display the detected faces
        if id_face is not None and webcam_face is not None:
            face_col1, face_col2 = st.columns(2)
            with face_col1:
                st.image(id_face, caption="Detected ID face", width=150)
            with face_col2:
                st.image(webcam_face, caption="Detected webcam face", width=150)
        else:
            st.error("Error: Face not detected in one or both images")
            
        # Face comparison with more realistic threshold
        if id_face is not None and webcam_face is not None:
            result["face_similarity"] = compare_faces(id_face, webcam_face)
            st.success(f"Face comparison similarity: {result['face_similarity']:.2f}")

        face_threshold = 0.55  # More realistic threshold
        
        # Calculate overall confidence - now includes role document match
        name_match_score = 0
        if result["id_name_match"]:
            name_match_score += 0.5
        if result["role_doc_match"]:
            name_match_score += 0.5
        else:
            # If role doc is provided but doesn't match, count as 0
            # If role doc is not provided, count as 0.5 (neutral)
            name_match_score += 0 if role_img is not None else 0.5
        
        result["confidence"] = result["face_similarity"] * 0.5 + name_match_score * 0.5
        
        # Determine if verification passed - now requires role document match if provided
        result["overall_verification"]["verified"] = (
            result["id_name_match"] and
            (role_img is None or result["role_doc_match"]) and  # Only check role doc if provided
            result["face_similarity"] > face_threshold and
            result["confidence"] > 0.45  # Lower threshold for testing
        )
        
        st.success(f"Verification result: {result['overall_verification']['verified']} with confidence {result['confidence']:.2f}")
        
    except Exception as e:
        st.error(f"Identity verification error: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        
    return result

# Streamlit App
def main():
    st.set_page_config(page_title="Document & Image Verification System", 
                       layout="wide", 
                       page_icon="🆔")
    
    st.title("🆔 Document & Image Verification System")
    
    st.markdown("""
    This system verifies identity by comparing an official ID document with a face photo and validating 
    entered information against extracted data.
    """)
    
    # Sidebar with options
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select Page", ["Identity Verification", "About"])
    
    if page == "About":
        st.markdown("""
        ## About This System
        
        This verification system provides multi-factor identity verification through:
        
        - Document text extraction using EasyOCR
        - Face detection and comparison
        - Name matching across documents
        - Official document validation
        
        The system supports multiple languages including English and Arabic.
        """)
        return
    
    # Main verification page
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("Step 1: Enter Personal Information")
        role = st.selectbox("Select Your Role", ["Player", "Player Agent", "Club Staff", "Service Provider"])
        user_name = st.text_input("Full Name (as shown on ID)")
        
        st.header("Step 2: Upload Documents")
        id_doc = st.file_uploader("Upload ID Document (Passport/CIN)", type=["jpg", "jpeg", "png"])
        
        doc_label = f"Upload {role} Document"
        role_doc = st.file_uploader(doc_label, type=["jpg", "jpeg", "png"])
        
    with col2:
       st.header("Step 3: Capture Face Photo")
       st.warning("Allow camera access when prompted")

    # First create a container for our camera and overlay
    st.markdown("""
    <style>
    /* Container for camera and overlay */
    .camera-container {
        position: relative;
        width: 100%;
        margin: 0 auto;
    }
    
    /* Face guide overlay positioned absolutely within the container */
    .face-guide-overlay {
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        pointer-events: none; /* Allow clicks to pass through to camera */
    }
    
    /* Circle overlay */
    .face-guide-circle {
        position: absolute;
        top: 50%;
        left: 50%;
        width: 220px;
        height: 220px;
        border: 4px dashed #FF4B4B;
        border-radius: 50%;
        transform: translate(-50%, -50%);
        z-index: 1000;
    }
    
    /* Instruction text */
    .face-guide-text {
        position: absolute;
        top: 20px;
        left: 50%;
        transform: translateX(-50%);
        color: white;
        background: rgba(255, 75, 75, 0.8);
        padding: 5px 15px;
        border-radius: 4px;
        font-weight: bold;
        z-index: 1001;
        white-space: nowrap;
    }
    </style>
    
    <div class="camera-container">
    """, unsafe_allow_html=True)
    
    # Add the camera input normally
    face_photo = st.camera_input("", key="face_camera")
    
    # Add the overlay after the camera is rendered
    st.markdown("""
        <div class="face-guide-overlay">
            <div class="face-guide-circle"></div>
            <div class="face-guide-text">Position your face here</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.info("Make sure your face is well lit and centered in the circle")
    # Process verification when all inputs are provided
    if st.button("Verify Identity", type="primary", disabled=(not user_name or not id_doc or not face_photo)):
        with st.spinner("Processing verification..."):
            # Verify if all required inputs are available
            if not user_name:
                st.error("Please enter your full name")
                return
            if not id_doc:
                st.error("Please upload your ID document")
                return
            if not face_photo:
                st.error("Please take a photo with your webcam")
                return
            
            # Load images
            id_image = load_image(id_doc)
            face_image = load_image(face_photo)
            
            if id_image is None or face_image is None:
                st.error("Error loading images. Please try again.")
                return
            
            # Display uploaded images
            col1, col2 = st.columns(2)
            with col1:
                st.image(id_image, caption="ID Document", use_container_width=True)
            with col2:
                st.image(face_image, caption="Webcam Photo", use_container_width=True)
            
            # Run OCR on ID document
            st.subheader("OCR Analysis")
            ocr_results = extract_text_with_easyocr(id_image)
            if ocr_results:
                ocr_viz = visualize_ocr_results(id_image, ocr_results)
                if ocr_viz is not None:
                    st.image(ocr_viz, caption="Text Detection Results", use_container_width=True)
                
                # Show extracted text
                st.subheader("Extracted Text")
                for _, text, confidence in ocr_results:
                    st.write(f"• {text} (Confidence: {confidence:.2f})")
            else:
                st.warning("No text was detected in the ID document")
            
            # Perform verification
            st.subheader("Verification Results")
            role_image = load_image(role_doc) if role_doc else None
            verification_result = verify_identity(id_image, face_image, role_image, user_name, role)
            
            # Show detailed results
            st.subheader("Identity Verification Summary")
            
            # Face verification
            if verification_result["face_similarity"] > 0.55:  # Realistic threshold
               st.success(f"✅ Face match successful ({verification_result['face_similarity']:.2f} similarity)")
            else:
               st.error(f"❌ Face match failed ({verification_result['face_similarity']:.2f} similarity)")
            
            # ID document name verification
            if verification_result["id_name_match"]:
               st.success("✅ ID document name match successful")
            else:
               st.error("❌ ID document name match failed")
    
            # Role document verification
            if role_doc is not None:
                if verification_result["role_doc_match"]:
                    st.success(f"✅ {role} document name match successful")
                else:
                    st.error(f"❌ {role} document name match failed")

            # Overall result
            st.markdown("---")
            if verification_result["overall_verification"]["verified"]:
                st.success(f"## ✅ IDENTITY VERIFICATION SUCCESSFUL ({verification_result['confidence']:.2f} confidence)")
            else:
                st.error(f"## ❌ IDENTITY VERIFICATION FAILED ({verification_result['confidence']:.2f} confidence)")

if __name__ == "__main__":
    main()