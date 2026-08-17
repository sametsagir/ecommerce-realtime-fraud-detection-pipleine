import os
import json
from datetime import datetime
try:
    import requests
except ImportError:
    requests = None
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, window, lit, current_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import TimestampType

# ----------------------------------------------------
# Global Configurations
# ----------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "orders"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Enforce secure configuration without hardcoded fallbacks
if not DB_USER or not DB_PASSWORD or not DB_NAME:
    raise ValueError("CRITICAL SECURITY ERROR: Database credentials ('DB_USER', 'DB_PASSWORD', 'DB_NAME') are not fully defined in environment variables!")

MSSQL_JDBC_URL = f"jdbc:sqlserver://mssql:1433;databaseName={DB_NAME};encrypt=true;trustServerCertificate=true;"
MSSQL_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError("CRITICAL SECURITY ERROR: MinIO credentials ('MINIO_ACCESS_KEY' / 'MINIO_SECRET_KEY') are not defined!")

SCHEMA_PATH = "/opt/spark/project/schemas/order_schema.avsc"


def create_spark_session():
    """
    Creates Spark Session and fetches dependencies from Maven:
    - spark-sql-kafka: Apache Kafka connector
    - spark-avro: Apache Avro format encoder/decoder
    - mssql-jdbc: Microsoft SQL Server JDBC driver
    - hadoop-aws: Amazon S3 (MinIO) filesystem support
    """
    return SparkSession.builder \
        .appName("RealTimeFraudDetector") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.spark:spark-avro_2.12:3.5.0,"
                "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()


def configure_s3_connection(spark):
    """Configures Hadoop S3A properties for MinIO storage integration."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def load_avro_schema():
    """Loads Avro schema string definition from disk."""
    with open(SCHEMA_PATH, "r") as f:
        return f.read()


def send_alert_to_discord(anomalies_list):
    """Dispatches real-time alerts to the configured Discord webhook channel."""
    if not requests:
        return
    discord_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not discord_url or "placeholder" in discord_url:
        return
        
    for row in anomalies_list:
        payload = {
            "embeds": [
                {
                    "title": "🚨 SUSPICIOUS TRANSACTION DETECTED",
                    "color": 15158332,  # Red
                    "fields": [
                        {"name": "Order ID", "value": str(row["order_id"]), "inline": False},
                        {"name": "User ID", "value": str(row["user_id"]), "inline": True},
                        {"name": "Reason", "value": f"`{row['anomaly_reason']}`", "inline": True},
                        {"name": "Amount", "value": f"{row['price']} TL", "inline": True},
                        {"name": "IP Address", "value": str(row["ip_address"]), "inline": True},
                        {"name": "Payment Method", "value": str(row["payment_method"]), "inline": True}
                    ],
                    "timestamp": datetime.utcnow().isoformat()
                }
            ]
        }
        try:
            requests.post(discord_url, json=payload, timeout=5)
        except Exception as e:
            print(f"Failed to send webhook alert: {str(e)}")


def write_anomalies_to_mssql(batch_df, batch_id):
    """
    Callback function triggered per micro-batch to write anomalies to SQL Server
    using staging tables and atomic MERGE to prevent primary key constraint violations.
    Also handles real-time alerts.
    """
    if batch_df.count() > 0:
        staging_table = f"supheli_siparisler_staging_{batch_id}"
        
        # Write batch data to a staging table (overwrite mode ensures no duplicate keys)
        batch_df.write \
            .mode("overwrite") \
            .jdbc(MSSQL_JDBC_URL, staging_table, properties=MSSQL_PROPERTIES)
        
        # Run upsert via MERGE command using the Java JDBC connection in Spark JVM
        spark = batch_df.sparkSession
        try:
            db_properties = spark._jvm.java.util.Properties()
            for k, v in MSSQL_PROPERTIES.items():
                db_properties.setProperty(k, v)
                
            driver_manager = spark._jvm.java.sql.DriverManager
            conn = driver_manager.getConnection(MSSQL_JDBC_URL, db_properties)
            stmt = conn.createStatement()
            
            # Execute atomic MERGE statement
            merge_sql = f"""
            MERGE INTO supheli_siparisler AS target
            USING {staging_table} AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.quantity = source.quantity, 
                    target.order_timestamp = source.order_timestamp
            WHEN NOT MATCHED THEN
                INSERT (order_id, user_id, product_id, price, quantity, order_timestamp, ip_address, payment_method, anomaly_reason)
                VALUES (source.order_id, source.user_id, source.product_id, source.price, source.quantity, source.order_timestamp, source.ip_address, source.payment_method, source.anomaly_reason);
            """
            stmt.execute(merge_sql)
            
            # Clean up the staging table
            stmt.execute(f"DROP TABLE {staging_table}")
            stmt.close()
            conn.close()
        except Exception as ex:
            print(f"Failed to upsert batch {batch_id} to SQL Server: {str(ex)}")
            # Cleanup staging table in case of failure
            try:
                conn = driver_manager.getConnection(MSSQL_JDBC_URL, db_properties)
                stmt = conn.createStatement()
                stmt.execute(f"IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL DROP TABLE {staging_table};")
                stmt.close()
                conn.close()
            except:
                pass
            raise ex
        
        # Limit collect calls to prevent driver OOM under high fraud rates
        alerts = batch_df.limit(5).collect()
        send_alert_to_discord(alerts)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    configure_s3_connection(spark)
    
    schema_str = load_avro_schema()

    # Establish stream connection from Kafka orders topic
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Strip the 5-byte Confluent wire protocol header (1 byte magic + 4 bytes schema ID)
    # to extract the clean raw Avro payload starting from index 6 (1-based index in Spark expression)
    stripped_df = kafka_stream_df.selectExpr(
        "substring(value, 6, length(value) - 5) as avro_value"
    )

    # Deserialize Avro bytes into structured columns using schema string definition
    parsed_df = stripped_df.select(
        from_avro(col("avro_value"), schema_str).alias("data")
    ).select("data.*")

    # Cast long epoch timestamp to TimestampType and define a 10-minute watermark for late data handling
    processed_df = parsed_df \
        .withColumn("order_timestamp", (col("timestamp") / 1000).cast(TimestampType())) \
        .withWatermark("order_timestamp", "10 minutes")

    # ----------------------------------------------------
    # Rule 1 & 2: Static Checks (Zero Price & Threshold Violations)
    # ----------------------------------------------------
    static_anomalies_df = processed_df \
        .filter((col("price") <= 0) | (col("price") >= 50000.0)) \
        .withColumn("anomaly_reason", 
                    expr("CASE WHEN price <= 0 THEN 'ZERO_PRICE' ELSE 'HIGH_AMOUNT' END")) \
        .select(
            col("order_id"), col("user_id"), col("product_id"), 
            col("price"), col("quantity"), col("order_timestamp"), 
            col("ip_address"), col("payment_method"), col("anomaly_reason")
        )

    # ----------------------------------------------------
    # Rule 3: Velocity Check (Sliding Time Windows)
    # ----------------------------------------------------
    # Group by sliding window, user_id and ip_address to identify bursts of activity
    window_spec = window(col("order_timestamp"), "10 seconds", "5 seconds")
    
    velocity_alerts_df = processed_df \
        .groupBy(window_spec.alias("win"), col("user_id"), col("ip_address")) \
        .count() \
        .filter(col("count") > 5)

    # Format aggregated velocity anomalies to align with SQL database schema without stream joins
    velocity_anomalies_df = velocity_alerts_df \
        .select(
            expr("CONCAT('VELOCITY-', user_id, '-', CAST(win.end AS STRING))").alias("order_id"),
            col("user_id"),
            lit("N/A").alias("product_id"),
            lit(0.0).alias("price"),
            col("count").alias("quantity"),
            col("win.end").alias("order_timestamp"),
            col("ip_address"),
            lit("credit_card").alias("payment_method"),
            lit("VELOCITY_ATTACK").alias("anomaly_reason")
        )

    # Extract raw transaction records (clean + anomalies) to write to S3 Data Lake (Bronze layer)
    raw_orders_df = processed_df.select(
        col("order_id"), col("user_id"), col("product_id"), 
        col("price"), col("quantity"), col("order_timestamp"), 
        col("ip_address"), col("payment_method")
    )

    # ----------------------------------------------------
    # Stream Sinks
    # ----------------------------------------------------

    # Sink A: Write static anomalies to MS SQL Server
    query_static_mssql = static_anomalies_df.writeStream \
        .queryName("MSSQLStaticAnomaliesSink") \
        .foreachBatch(write_anomalies_to_mssql) \
        .option("checkpointLocation", "/opt/spark/project/checkpoints/mssql_static_anomalies") \
        .outputMode("append") \
        .start()

    # Sink B: Write velocity anomalies to MS SQL Server
    query_velocity_mssql = velocity_anomalies_df.writeStream \
        .queryName("MSSQLVelocityAnomaliesSink") \
        .foreachBatch(write_anomalies_to_mssql) \
        .option("checkpointLocation", "/opt/spark/project/checkpoints/mssql_velocity_anomalies") \
        .outputMode("update") \
        .start()

    # Sink C: Write all transaction records to MinIO (S3 Data Lake) as Parquet
    query_minio = raw_orders_df.writeStream \
        .queryName("MinIORawOrdersSink") \
        .format("parquet") \
        .option("path", "s3a://ecommerce-lake/orders") \
        .option("checkpointLocation", "/opt/spark/project/checkpoints/minio_orders") \
        .outputMode("append") \
        .start()

    query_static_mssql.awaitTermination()
    query_velocity_mssql.awaitTermination()
    query_minio.awaitTermination()


if __name__ == "__main__":
    main()
