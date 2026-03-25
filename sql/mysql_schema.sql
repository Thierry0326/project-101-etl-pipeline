-- ============================================
-- PROJECT 101 - MYSQL SCHEMA
-- SILVER LAYER: Normalized (stackoverflow_processed)
-- GOLD LAYER:   Star Schema (stackoverflow_analytics)
-- ============================================


-- ============================================
-- SILVER LAYER 🥈: NORMALIZED DATABASE
-- Purpose: Clean, typed, relational data
-- Accessible by: Everyone
-- ============================================
CREATE DATABASE IF NOT EXISTS stackoverflow_processed;
USE stackoverflow_processed;

CREATE TABLE IF NOT EXISTS respondents (
    respondent_id       INT PRIMARY KEY,
    main_branch         VARCHAR(100),
    is_hobbyist         BOOLEAN,
    age_range           VARCHAR(50),
    gender              VARCHAR(100),
    country             VARCHAR(100),
    employment_status   VARCHAR(100),
    org_size            VARCHAR(100),
    years_coding        INT,
    years_coding_pro    INT,
    work_week_hrs       DECIMAL(5,2),
    job_satisfaction    VARCHAR(100),
    career_satisfaction VARCHAR(100),
    operating_system    VARCHAR(100),
    survey_year         INT DEFAULT 2020,
    processed_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS respondent_education (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id       INT,
    education_level     VARCHAR(150),
    undergraduate_major VARCHAR(150),
    FOREIGN KEY (respondent_id) REFERENCES respondents(respondent_id)
);

CREATE TABLE IF NOT EXISTS respondent_compensation (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id   INT,
    currency        VARCHAR(100),
    comp_total      DECIMAL(15,2),
    comp_frequency  VARCHAR(50),
    FOREIGN KEY (respondent_id) REFERENCES respondents(respondent_id)
);

CREATE TABLE IF NOT EXISTS respondent_technologies (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id       INT,
    tech_type           VARCHAR(50),
    tech_name           VARCHAR(100),
    is_currently_using  BOOLEAN,
    is_desired_next_yr  BOOLEAN,
    FOREIGN KEY (respondent_id) REFERENCES respondents(respondent_id)
);

CREATE TABLE IF NOT EXISTS respondent_dev_types (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id   INT,
    dev_type        VARCHAR(150),
    FOREIGN KEY (respondent_id) REFERENCES respondents(respondent_id)
);

-- Silver layer indexes
CREATE INDEX idx_country ON respondents(country);
CREATE INDEX idx_employment ON respondents(employment_status);
CREATE INDEX idx_tech_name ON respondent_technologies(tech_name);
CREATE INDEX idx_comp_total ON respondent_compensation(comp_total);


-- ============================================
-- GOLD LAYER 🥇: STAR SCHEMA DATABASE
-- Purpose: Optimized for Power BI & Analytics
-- Accessible by: Data Analysts & BI Tools
-- ============================================
CREATE DATABASE IF NOT EXISTS stackoverflow_analytics;
USE stackoverflow_analytics;

-- --------------------------------------------
-- FACT TABLE: Core survey metrics
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS fact_survey_responses (
    fact_id             INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id       INT,
    developer_key       INT,
    geography_key       INT,
    education_key       INT,
    compensation_key    INT,
    years_coding        INT,
    years_coding_pro    INT,
    work_week_hrs       DECIMAL(5,2),
    comp_total_usd      DECIMAL(15,2),
    job_satisfaction_score   TINYINT,
    career_satisfaction_score TINYINT,
    is_hobbyist         BOOLEAN,
    survey_year         INT DEFAULT 2020,
    processed_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------
-- DIMENSION: Developer profile
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS dim_developer (
    developer_key       INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id       INT,
    main_branch         VARCHAR(100),
    employment_status   VARCHAR(100),
    org_size            VARCHAR(100),
    operating_system    VARCHAR(100),
    age_range           VARCHAR(50),
    gender              VARCHAR(100)
);

-- --------------------------------------------
-- DIMENSION: Geography
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_key       INT AUTO_INCREMENT PRIMARY KEY,
    country             VARCHAR(100),
    continent           VARCHAR(50),
    region              VARCHAR(100)
);

-- --------------------------------------------
-- DIMENSION: Education
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS dim_education (
    education_key       INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id       INT,
    education_level     VARCHAR(150),
    undergraduate_major VARCHAR(150)
);

-- --------------------------------------------
-- DIMENSION: Technology
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS dim_technology (
    technology_key      INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id       INT,
    tech_type           VARCHAR(50),
    tech_name           VARCHAR(100),
    is_currently_using  BOOLEAN,
    is_desired_next_yr  BOOLEAN
);

-- --------------------------------------------
-- DIMENSION: Compensation
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS dim_compensation (
    compensation_key    INT AUTO_INCREMENT PRIMARY KEY,
    respondent_id       INT,
    currency            VARCHAR(100),
    comp_total          DECIMAL(15,2),
    comp_frequency      VARCHAR(50),
    comp_total_usd      DECIMAL(15,2)
);

-- Gold layer indexes (optimized for Power BI)
CREATE INDEX idx_fact_developer ON fact_survey_responses(developer_key);
CREATE INDEX idx_fact_geography ON fact_survey_responses(geography_key);
CREATE INDEX idx_fact_education ON fact_survey_responses(education_key);
CREATE INDEX idx_fact_compensation ON fact_survey_responses(compensation_key);
CREATE INDEX idx_dim_country ON dim_geography(country);
CREATE INDEX idx_dim_tech ON dim_technology(tech_name);
CREATE INDEX idx_dim_comp ON dim_compensation(comp_total_usd);

-- Airflow metadata database
CREATE DATABASE IF NOT EXISTS airflow_metadata;