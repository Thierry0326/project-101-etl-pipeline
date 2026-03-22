-- ============================================
-- PROJECT 101 - DATABASE SECURITY SETUP
-- Run this AFTER creating schemas
-- ============================================


-- ============================================
-- SQL SERVER SECURITY (Run in SSMS)
-- ============================================

-- Create ETL login (for Python pipeline)
-- Has INSERT & SELECT only
CREATE LOGIN etl_user 
    WITH PASSWORD = 'ETL@Project101!';

CREATE USER etl_user 
    FOR LOGIN etl_user;

GRANT SELECT, INSERT 
    ON SCHEMA::dbo TO etl_user;

-- Create read-only login (for reporting tools)
CREATE LOGIN readonly_user 
    WITH PASSWORD = 'ReadOnly@Project101!';

CREATE USER readonly_user 
    FOR LOGIN readonly_user;

GRANT SELECT 
    ON SCHEMA::dbo TO readonly_user;
GO


-- ============================================
-- MYSQL SECURITY (Run in MySQL Workbench)
-- ============================================

-- ETL user for Python pipeline (Silver layer)
CREATE USER 'etl_user'@'%' 
    IDENTIFIED BY 'ETL@Project101!';

GRANT SELECT, INSERT, UPDATE 
    ON stackoverflow_processed.* 
    TO 'etl_user'@'%';

-- Analytics user for Power BI (Gold layer - READ ONLY)
CREATE USER 'analytics_user'@'%' 
    IDENTIFIED BY 'Analytics@Project101!';

GRANT SELECT 
    ON stackoverflow_analytics.* 
    TO 'analytics_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;


-- ============================================
-- BACKUP SCHEDULE NOTES
-- ============================================
-- SQL Server: Use SQL Server Agent (when on WiFi)
-- MySQL: Use mysqldump via Python scheduled script
-- Backup location: /backups folder in project
-- Schedule: Daily at midnight via Airflow DAG
-- Retention: Keep last 7 days of backups