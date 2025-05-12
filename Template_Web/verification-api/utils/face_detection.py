import cv2
import numpy as np
import logging
import random  # For demo purposes

class FaceDetector:
    """Face detection utility class"""
    
    def __init__(self):
        # Initialize face detector
        self.face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
        
    def detect_faces(self, image):
        """
        Detect faces in an image
        
        Args:
            image: RGB image as numpy array
            
        Returns:
            list: List of face rectangles (x, y, width, height)
        """
        if image is None:
            return []
            
        # Convert to grayscale for face detection
        gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
        
        # Detect faces
        faces = self.face_cascade.detectMultiScale(
            gray,
            scaleFactor=1.1,
            minNeighbors=5,
            minSize=(30, 30)
        )
        
        return faces
        
    def extract_face(self, image, padding=0.2):
        """
        Extract the largest face from an image with padding
        
        Args:
            image: RGB image as numpy array
            padding (float): Padding factor to add around the face
            
        Returns:
            numpy.ndarray: Cropped face image or None if no face detected
        """
        faces = self.detect_faces(image)
        
        if len(faces) == 0:
            return None
            
        # Find the largest face
        largest_face = max(faces, key=lambda rect: rect[2] * rect[3])
        x, y, w, h = largest_face
        
        # Add padding
        pad_w = int(w * padding)
        pad_h = int(h * padding)
        
        # Calculate new coordinates with padding
        x1 = max(0, x - pad_w)
        y1 = max(0, y - pad_h)
        x2 = min(image.shape[1], x + w + pad_w)
        y2 = min(image.shape[0], y + h + pad_h)
        
        # Extract face region
        face_image = image[y1:y2, x1:x2]
        
        return face_image

def compare_faces_opencv(id_image, selfie_image):
    """
    Compare faces in ID document and selfie using OpenCV
    
    Args:
        id_image: RGB image from ID document
        selfie_image: RGB image from selfie
        
    Returns:
        dict: Comparison results with match status and confidence
    """
    try:
        # Initialize face detector
        detector = FaceDetector()
        
        # Extract faces
        id_face = detector.extract_face(id_image)
        selfie_face = detector.extract_face(selfie_image)
        
        # Check if faces were detected
        if id_face is None:
            return {"match": False, "confidence": 0, "error": "No face detected in ID document"}
            
        if selfie_face is None:
            return {"match": False, "confidence": 0, "error": "No face detected in selfie"}
            
        # In a real implementation, we would use face recognition here
        # For demo purposes, we'll simulate the comparison result
        
        # For demo purposes, consider a match with 80% probability
        is_match = random.random() < 0.8
        
        if is_match:
            confidence = random.uniform(0.75, 0.98)
            return {
                "match": True,
                "confidence": confidence,
                "message": f"Face verification successful (confidence: {confidence:.2f})"
            }
        else:
            confidence = random.uniform(0.3, 0.7)
            return {
                "match": False, 
                "confidence": confidence,
                "message": f"Face verification failed (confidence: {confidence:.2f})"
            }
            
    except Exception as e:
        logging.error(f"Error comparing faces: {str(e)}")
        return {"match": False, "confidence": 0, "error": str(e)}