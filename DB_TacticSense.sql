-- FULL TACTICSENSE DATABASE (FINAL VERSION)

-- 1. USERS TABLE
CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    stakeholder_type ENUM('Player', 'Scout', 'Agent', 'Club', 'Sponsor', 'ServiceProvider', 'CommunicationAgency', 'SportsManagementAgency', 'EquipmentSupplier', 'RecruitingAgent', 'FitnessClub', 'TravelAgency', 'SportsClothingBrand', 'ManagerStaff') NOT NULL,
    username VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255),
    nationality VARCHAR(100),
    birthdate DATE,
    profile_photo_url VARCHAR(255),
    visibility ENUM('Public', 'Private') DEFAULT 'Public',
    verified_status BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. LIKES TABLE
CREATE TABLE likes (
    like_id INT AUTO_INCREMENT PRIMARY KEY,
    liker_user_id INT NOT NULL,
    liked_entity_id INT NOT NULL,
    liked_entity_type ENUM('Player', 'Club', 'Agent', 'Scout', 'Sponsor', 'ServiceProvider', 'FitnessClub', 'TravelAgency', 'CommunicationAgency', 'EquipmentSupplier', 'SportsManagementAgency', 'SportsClothingBrand') NOT NULL,
    like_points INT NOT NULL,
    liked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (liker_user_id) REFERENCES users(user_id)
);

-- 3. MEDICAL HISTORY TABLE
CREATE TABLE medical_history (
    medical_id INT AUTO_INCREMENT PRIMARY KEY,
    player_id INT NOT NULL,
    injury_type VARCHAR(255),
    injury_date DATE,
    recovery_date DATE,
    surgery BOOLEAN DEFAULT FALSE,
    chronic_issue BOOLEAN DEFAULT FALSE,
    notes TEXT,
    FOREIGN KEY (player_id) REFERENCES users(user_id)
);

-- 4. PLAYERS TABLE
CREATE TABLE players (
    player_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    position VARCHAR(100),
    club_id INT,
    market_value DECIMAL(15,2),
    scouting_score DECIMAL(5,2),
    tactic_fit_score DECIMAL(5,2),
    skills JSON,
    profile_rating DECIMAL(5,2),
    age INT,
    verified_status BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 5. CLUBS TABLE
CREATE TABLE clubs (
    club_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100),
    league VARCHAR(100),
    formation_preference VARCHAR(20),
    playing_style VARCHAR(100),
    salary_budget DECIMAL(15,2),
    market_budget DECIMAL(15,2),
    scouting_focus VARCHAR(255),
    recruitment_priority VARCHAR(255),
    stadium VARCHAR(255),
    transfer_history JSON,
    social_media_links JSON,
    sponsorship_deals JSON,
    verified_status BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 6. PLAYER AGENTS TABLE
CREATE TABLE player_agents (
    agent_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    license_number VARCHAR(100),
    affiliated_players JSON,
    years_experience INT,
    verified_status BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 7. RECRUITING AGENTS TABLE
CREATE TABLE recruiting_agents (
    scout_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    full_name VARCHAR(255),
    region VARCHAR(100),
    experience_years INT,
    verified_status BOOLEAN DEFAULT FALSE,
    successful_signings INT,
    preferred_positions JSON,
    performance_score DECIMAL(10,2),
    contact_email VARCHAR(255),
    phone_number VARCHAR(20),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 8. SPONSORS TABLE
CREATE TABLE sponsors (
    sponsor_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    location VARCHAR(100),
    industry VARCHAR(100),
    sponsorship_deals JSON,
    contract_value DECIMAL(15,2),
    advertising_channels JSON,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 9. SERVICE PROVIDERS TABLE
CREATE TABLE service_providers (
    provider_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    service_type VARCHAR(255),
    region VARCHAR(100),
    partnerships_made VARCHAR(255),
    contact_email VARCHAR(255),
    phone_number VARCHAR(20),
    website VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 10. COMMUNICATION AGENCIES TABLE
CREATE TABLE communication_agencies (
    agency_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    company_name VARCHAR(255),
    region VARCHAR(100),
    services_offered TEXT,
    affiliated_clients JSON,
    partnerships_made JSON,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 11. SPORTS MANAGEMENT AGENCIES TABLE
CREATE TABLE sports_management_agencies (
    agency_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    agency_name VARCHAR(255),
    region VARCHAR(100),
    services_offered TEXT,
    top_clients JSON,
    financial_services TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 12. FITNESS CLUBS TABLE
CREATE TABLE fitness_clubs (
    fitness_club_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    club_name VARCHAR(255),
    services_offered TEXT,
    affiliated_players JSON,
    certified_trainers JSON,
    membership_fees DECIMAL(10,2),
    contact_email VARCHAR(255),
    phone_number VARCHAR(20),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 13. EQUIPMENT SUPPLIERS TABLE
CREATE TABLE equipment_suppliers (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    company_name VARCHAR(255),
    product_type VARCHAR(255),
    price DECIMAL(10,2),
    affiliated_clients JSON,
    operating_region VARCHAR(100),
    contact_email VARCHAR(255),
    phone_number VARCHAR(20),
    website VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 14. SPORTS CLOTHING BRANDS TABLE
CREATE TABLE sports_clothing_brands (
    brand_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    brand_name VARCHAR(255),
    location VARCHAR(100),
    product_types JSON,
    annual_revenue DECIMAL(15,2),
    sponsorship_deals JSON,
    contact_email VARCHAR(255),
    phone_number VARCHAR(20),
    website VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 15. TRAVEL AGENCIES TABLE
CREATE TABLE travel_agencies (
    travel_agency_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    company_name VARCHAR(255),
    services_offered TEXT,
    affiliated_clients JSON,
    operating_region VARCHAR(100),
    travel_partners JSON,
    contact_email VARCHAR(255),
    phone_number VARCHAR(20),
    website VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 16. MANAGERS & STAFF TABLE
CREATE TABLE managers_and_staff (
    manager_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE,
    full_name VARCHAR(255),
    function_group VARCHAR(100),
    function_role VARCHAR(100),
    certifications JSON,
    since DATE,
    contract_until DATE,
    years_experience INT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 17. POINTS SETTINGS TABLE (for likes)
CREATE TABLE points_settings (
    entity_type ENUM('Player', 'Club', 'Agent', 'Scout', 'Sponsor', 'ServiceProvider', 'FitnessClub', 'TravelAgency', 'CommunicationAgency', 'EquipmentSupplier', 'SportsManagementAgency', 'SportsClothingBrand') PRIMARY KEY,
    points INT NOT NULL
);
