**Project: Stock Data Pipeline to Snowflake
**
This project implements an automated ETL pipeline in Python that fetches historical stock data from the National Stock Exchange (NSE) via Yahoo Finance using the yfinance library and loads it into Snowflake for analytics and reporting.

The pipeline is designed for performance and scalability using parallel processing with Python’s ThreadPoolExecutor, enabling faster extraction and transformation of data for multiple stock symbols concurrently. Transformed data is written into Parquet files, which are then ingested into Snowflake using the PUT and COPY INTO commands for efficient and optimized loading.

The Snowflake data warehouse schema includes well-structured tables:

stock_symbol – stores metadata of each stock,

stock_prices_parallel_load – contains time-series historical stock prices,

DATA_LOAD_LOG – a custom logging table to track the status of each pipeline run.

The pipeline features a robust logging and monitoring system that captures start and end times, execution status (success/failure), and detailed error messages for debugging and audit purposes. This ensures transparency in data movement and helps maintain data integrity.

Overall, the project showcases how to build a high-performance, fault-tolerant data pipeline to Snowflake, suitable for financial data engineering use cases involving large-scale, time-series stock data.
