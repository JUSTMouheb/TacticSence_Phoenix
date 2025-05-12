import re
from unidecode import unidecode

def normalize_name(name):
    """
    Normalize a name for comparison by removing diacritics, special characters,
    and standardizing common name variations
    
    Args:
        name: The name to normalize
    
    Returns:
        str: Normalized name
    """
    if not name:
        return ""
    
    # Check if name contains Arabic characters
    is_arabic = any('\u0600' <= c <= '\u06FF' for c in name)
    
    if is_arabic:
        # Remove Arabic diacritics and special characters
        normalized = ''.join(c for c in name if not '\u064B' <= c <= '\u0652')
        normalized = normalized.replace('\u0640', '')  # Remove tatweel
        normalized = normalized.lower()
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized.strip()
    else:
        # Process Latin script names
        name = name.lower()
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'[^\w\s]', '', name)
        
        # Common name equivalents
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

def compare_names(name1, name2):
    """
    Compare two names and determine if they match
    
    Args:
        name1: First name
        name2: Second name
    
    Returns:
        dict: Comparison results with match status and similarity score
    """
    if not name1 or not name2:
        return {"match": False, "similarity": 0.0}
    
    # Check if names have different scripts (Arabic vs. Latin)
    is_arabic1 = any('\u0600' <= c <= '\u06FF' for c in name1)
    is_arabic2 = any('\u0600' <= c <= '\u06FF' for c in name2)
    
    # If script mismatch, use special comparison
    if is_arabic1 != is_arabic2:
        norm1 = normalize_name(name1)
        norm2 = normalize_name(name2)
        
        # Check if shorter name is contained in longer name
        shorter = norm1 if len(norm1) < len(norm2) else norm2
        longer = norm2 if len(norm1) < len(norm2) else norm1
        
        if shorter in longer:
            similarity = len(shorter) / len(longer)
            return {"match": similarity > 0.5, "similarity": similarity}
        
        # Check for word-level intersection
        words1 = set(norm1.split())
        words2 = set(norm2.split())
        intersection = len(words1.intersection(words2))
        
        if intersection > 0:
            similarity = intersection / max(len(words1), len(words2))
            return {"match": similarity > 0.3, "similarity": similarity}
        
        return {"match": False, "similarity": 0.1}
    
    # Same script comparison
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)
    
    if norm1 == norm2:
        return {"match": True, "similarity": 1.0}
    
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    # Adjust threshold based on script
    threshold = 0.5 if is_arabic1 else 0.7
    
    # Check if one name is a subset of the other
    if words1.issubset(words2) or words2.issubset(words1):
        common_words = words1.intersection(words2)
        total_words = max(len(words1), len(words2))
        
        if total_words > 0:
            similarity = len(common_words) / total_words
            return {"match": similarity > threshold, "similarity": similarity}
    
    # General case - Jaccard similarity
    if not words1 or not words2:
        return {"match": False, "similarity": 0.0}
    
    intersection = len(words1.intersection(words2))
    union = len(words1.union(words2))
    
    similarity = intersection / union if union > 0 else 0.0
    return {"match": similarity > threshold, "similarity": similarity}