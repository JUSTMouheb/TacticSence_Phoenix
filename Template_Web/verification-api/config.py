import os

class Config:
    """Base configuration"""
    DEBUG = False
    TESTING = False
    UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploaded_documents')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16 MB max upload size
    
class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    
class ProductionConfig(Config):
    """Production configuration"""
    # Production specific settings
    pass
    
class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    
# Create uploads folder
os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}