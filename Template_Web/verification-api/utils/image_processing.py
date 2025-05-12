import re
import cv2
import logging
from .helpers import load_image, preprocess_image
from .ocr import extract_text_with_easyocr

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_identity_document(image_path):
    """
    Validates if the uploaded image is likely to be a valid identity document.
    
    Args:
        image_path: Path to the image file
        
    Returns:
        dict: Dictionary with validation results
    """
    result = {
        "valid": False,
        "confidence": 0.0,
        "detected_elements": [],
        "document_type": "unknown",
        "reason": "",
        "raw_text": ""
    }
    
    try:
        # Load and preprocess image
        image = load_image(image_path)
        if image is None:
            return {
                "valid": False,
                "reason": "Failed to load image",
                "confidence": 0.0,
                "detected_elements": [],
                "raw_text": ""
            }
        
        processed_image, gray, _ = preprocess_image(image)
        if processed_image is None:
            return {
                "valid": False,
                "reason": "Failed to preprocess image",
                "confidence": 0.0,
                "detected_elements": [],
                "raw_text": ""
            }
        
        # Extract text with OCR
        ocr_results = extract_text_with_easyocr(processed_image)
        if not ocr_results:
            # Try with original image if processed image failed
            ocr_results = extract_text_with_easyocr(image)
            if not ocr_results:
                return {
                    "valid": False,
                    "reason": "No text detected in document",
                    "confidence": 0.0,
                    "detected_elements": [],
                    "raw_text": ""
                }
        
        # Extract all text and prepare a normalized version for matching
        all_texts = [text.lower() for _, text, _ in ocr_results]
        all_text = " ".join(all_texts)
        result["raw_text"] = all_text
        
        # Strip diacritics and special characters for Arabic text normalization
        normalized_text = re.sub(r'[\u064B-\u0652\u0640]', '', all_text)
        
        # Define dictionaries of common elements found in identity documents by country/region
        id_elements = {
            # Generic ID elements (most IDs worldwide)
            "generic": {
                "keywords": [
                    "identity", "carte", "id card", "identification", "national", "republic",
                    "passport", "citizen", "official", "government", "date of birth", "birthdate",
                    "date de naissance", "né le", "nationality", "nationalité", "sex", "sexe", "gender",
                    "expiry", "expiration", "issued", "délivré", "signature", "card", "identity card"
                ],
                "patterns": [
                    r'\b[A-Z0-9]{7,15}\b',  # Generic ID number pattern
                    r'\b\d{2}[/.-]\d{2}[/.-]\d{4}\b',  # Date format DD/MM/YYYY
                    r'\b\d{4}[/.-]\d{2}[/.-]\d{2}\b',  # Date format YYYY/MM/DD
                ]
            },
            
            # Tunisia-specific elements
            "tunisia": {
                "keywords": [
                    "الجمهورية التونسية", "république tunisienne", "republic of tunisia",
                    "بطاقة تعريف وطنية", "carte d'identité nationale", "national identity card",
                    "carte nationale", "identite nationale", "تونس", "tunisie", "tunisia",
                    "تعريف", "republique", "الوطنية", "وطنية", "الجمهورية", "تونسية",
                    "هوية", "بطاقة"
                ],
                "patterns": [
                    r'\b\d{8}\b',  # 8-digit Tunisian CIN number
                ]
            },
            
            # Morocco-specific elements
            "morocco": {
                "keywords": [
                    "المملكة المغربية", "royaume du maroc", "kingdom of morocco",
                    "البطاقة الوطنية للتعريف", "carte nationale d'identité",
                    "المغرب", "maroc", "morocco"
                ],
                "patterns": [
                    r'\b[A-Z]{1,2}\d{5,7}\b',  # Moroccan ID format
                ]
            },
            
            # Algeria-specific elements
            "algeria": {
                "keywords": [
                    "الجمهورية الجزائرية", "république algérienne", "people's democratic republic of algeria",
                    "بطاقة التعريف الوطنية", "carte d'identité nationale", "national identity card",
                    "الجزائر", "algérie", "algeria"
                ],
                "patterns": [
                    r'\b\d{18}\b',  # 18-digit Algerian ID format
                ]
            },
            
            # Egypt-specific elements
            "egypt": {
                "keywords": [
                    "جمهورية مصر العربية", "arab republic of egypt",
                    "بطاقة تحقيق الشخصية", "بطاقة الرقم القومي",
                    "مصر", "egypt"
                ],
                "patterns": [
                    r'\b\d{14}\b',  # 14-digit Egyptian National ID
                ]
            },
            
            # Anti-patterns (indicators it's NOT an ID)
            "anti_patterns": [
                "invoice", "receipt", "facture", "ticket", "reservation", "booking",
                "menu", "brochure", "advertisement", "publicité"
            ]
        }
        
        # Track matched elements and final confidence
        matched_elements = []
        confidence = 0.0
        country = "unknown"
        
        # Check for anti-patterns first (things that suggest it's NOT an ID)
        for anti_pattern in id_elements["anti_patterns"]:
            if anti_pattern.lower() in normalized_text:
                # Full match, probably not an ID
                result["reason"] = f"Document contains '{anti_pattern}', likely not an ID"
                result["confidence"] = 0.1
                return result
        
        # Check country-specific elements (with higher weights)
        for country_name, elements in id_elements.items():
            if country_name == "anti_patterns" or country_name == "generic":
                continue
            
            # Check country-specific keywords
            country_matched = False
            if "keywords" in elements:
                for keyword in elements["keywords"]:
                    # Try direct match first
                    if keyword.lower() in all_text:
                        matched_elements.append(f"{country_name}: {keyword}")
                        confidence += 0.15  # Country-specific keywords have higher weight
                        country_matched = True
                        country = country_name
                        continue
                    
                    # Try normalized match for Arabic
                    norm_keyword = re.sub(r'[\u064B-\u0652\u0640]', '', keyword.lower())
                    if norm_keyword in normalized_text:
                        matched_elements.append(f"{country_name}: {keyword} (normalized)")
                        confidence += 0.15  # Country-specific keywords have higher weight
                        country_matched = True
                        country = country_name
                        continue
            
            # Check country-specific patterns
            if "patterns" in elements:
                for pattern in elements["patterns"]:
                    matches = re.findall(pattern, all_text)
                    if matches:
                        matched_elements.append(f"{country_name}_pattern: {matches[0]}")
                        confidence += 0.25  # ID number patterns have highest weight
                        if not country_matched:
                            country = country_name
        
        # Check generic ID elements
        for keyword in id_elements["generic"]["keywords"]:
            if keyword.lower() in all_text:
                matched_elements.append(f"generic: {keyword}")
                confidence += 0.05  # Generic keywords have lower weight
        
        for pattern in id_elements["generic"]["patterns"]:
            matches = re.findall(pattern, all_text)
            if matches:
                matched_elements.append(f"generic_pattern: {matches[0]}")
                confidence += 0.1
        
        # Check for numerical patterns common in IDs
        numeric_patterns = re.findall(r'\b\d{6,}\b', all_text)  # Find any sequence of 6+ digits
        if numeric_patterns:
            matched_elements.append(f"numeric_sequence: {numeric_patterns[0]}")
            confidence += 0.1
            
        # Special case for Tunisian IDs
        tunisia_keywords_detected = any(item.startswith("tunisia:") for item in matched_elements)
        if tunisia_keywords_detected and confidence < 0.2:
            confidence = max(confidence, 0.2)
            matched_elements.append("tunisia: base confidence boost")
        
        # Calculate final result
        result["detected_elements"] = matched_elements
        result["confidence"] = min(confidence, 1.0)  # Cap at 1.0
        result["document_type"] = f"{country} ID" if country != "unknown" else "ID document"
        
        # Reduced threshold to 0.2 to be more lenient
        if confidence >= 0.2 or (confidence >= 0.1 and country != "unknown"):
            result["valid"] = True
            result["reason"] = f"Verified as {result['document_type']} with {result['confidence']:.2f} confidence"
        else:
            result["valid"] = False
            if matched_elements:
                result["reason"] = f"Low confidence score ({result['confidence']:.2f}) for ID verification"
            else:
                result["reason"] = "No identity document markers detected"
        
        return result
    
    except Exception as e:
        logger.error(f"Error validating identity document: {str(e)}")
        return {
            "valid": False,
            "confidence": 0.0,
            "detected_elements": [],
            "reason": f"Error during validation: {str(e)}",
            "raw_text": ""
        }