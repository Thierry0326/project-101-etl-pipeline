-- ============================================
-- PROJECT 101 - SQL SERVER RAW/STAGING SCHEMA
-- Stack Overflow 2020 Developer Survey
-- ============================================

-- Create the staging database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'stackoverflow_raw')
BEGIN
    CREATE DATABASE stackoverflow_raw;
END
GO

USE stackoverflow_raw;
GO

-- Drop table if exists (clean slate)
IF OBJECT_ID('dbo.survey_responses_raw', 'U') IS NOT NULL
    DROP TABLE dbo.survey_responses_raw;
GO

-- ============================================
-- RAW SURVEY RESPONSES TABLE
-- All columns stored as NVARCHAR (raw/untouched)
-- We clean types in the transform phase
-- ============================================
CREATE TABLE dbo.survey_responses_raw (
    -- System columns
    raw_id          INT IDENTITY(1,1) PRIMARY KEY,
    loaded_at       DATETIME DEFAULT GETDATE(),

    -- Survey metadata
    respondent_id   NVARCHAR(50),
    main_branch     NVARCHAR(255),
    hobbyist        NVARCHAR(50),

    -- Demographics
    age             NVARCHAR(50),
    gender          NVARCHAR(255),
    country         NVARCHAR(255),
    ethnicity       NVARCHAR(500),
    
    -- Education
    ed_level        NVARCHAR(255),
    undergraduate_major NVARCHAR(255),

    -- Employment
    employment      NVARCHAR(255),
    org_size        NVARCHAR(255),
    dev_type        NVARCHAR(500),
    years_code      NVARCHAR(50),
    years_code_pro  NVARCHAR(50),

    -- Compensation
    currency        NVARCHAR(255),
    comp_total      NVARCHAR(100),
    comp_freq       NVARCHAR(100),

    -- Technologies
    language_worked_with        NVARCHAR(2000),
    language_desired_next_year  NVARCHAR(2000),
    database_worked_with        NVARCHAR(2000),
    database_desired_next_year  NVARCHAR(2000),
    platform_worked_with        NVARCHAR(2000),
    platform_desired_next_year  NVARCHAR(2000),
    web_frame_worked_with       NVARCHAR(2000),
    web_frame_desired_next_year NVARCHAR(2000),

    -- Job satisfaction
    job_sat         NVARCHAR(255),
    career_sat      NVARCHAR(255),
    work_week_hrs   NVARCHAR(50),

    -- Remote work
    op_sys          NVARCHAR(255),
    
    -- Source tracking
    survey_year     INT DEFAULT 2020
);
GO

-- ============================================
-- INDEX for faster queries
-- ============================================
CREATE INDEX idx_country 
    ON dbo.survey_responses_raw(country);

CREATE INDEX idx_loaded_at 
    ON dbo.survey_responses_raw(loaded_at);
GO

-- Confirm creation
SELECT 'stackoverflow_raw database and survey_responses_raw table created successfully!' AS status;
GO