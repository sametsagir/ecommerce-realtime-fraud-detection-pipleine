-- ====================================================
-- Database Initialization for Microsoft SQL Server
-- ====================================================
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'ecommerce_fraud_db')
BEGIN
    CREATE DATABASE ecommerce_fraud_db;
END
GO

USE ecommerce_fraud_db;
GO

-- ====================================================
-- Suspicious Orders Table (Real-time anomaly output)
-- ====================================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='supheli_siparisler' AND xtype='U')
BEGIN
    CREATE TABLE supheli_siparisler (
        order_id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        price FLOAT NOT NULL,
        quantity INT NOT NULL,
        order_timestamp DATETIME2 NOT NULL,
        ip_address VARCHAR(50) NOT NULL,
        payment_method VARCHAR(50) NOT NULL,
        anomaly_reason VARCHAR(255) NOT NULL,
        detected_at DATETIME2 DEFAULT GETDATE()
    );

    -- Non-clustered indexes for dashboard query optimization
    CREATE INDEX idx_anomaly_timestamp ON supheli_siparisler(order_timestamp);
    CREATE INDEX idx_anomaly_reason ON supheli_siparisler(anomaly_reason);
END
GO

-- ====================================================
-- Daily Audit Summary Table (Batch report output)
-- ====================================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='gunluk_ozet' AND xtype='U')
BEGIN
    CREATE TABLE gunluk_ozet (
        summary_date DATE PRIMARY KEY,
        total_orders INT NOT NULL,
        total_clean_revenue FLOAT NOT NULL,
        total_fraud_count INT NOT NULL,
        fraud_ratio FLOAT NOT NULL,
        created_at DATETIME2 DEFAULT GETDATE()
    );
END
GO
