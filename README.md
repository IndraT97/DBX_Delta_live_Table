# DBX_Delta_live_Table
Used Concept of Autoloader, SCD's and Delta Live Table to build automated and efficient Data Pipeline

<p align="center">
  <img src="https://github.com/IndraT97/DBX_Delta_live_Table/blob/main/lineage.png">
</p>

# Delta Live Table

Delta Live Tables is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define the transformations to perform on your data and Delta Live Tables manages task orchestration, cluster management, monitoring, data quality, and error handling.

Instead of defining your data pipelines using a series of separate Apache Spark tasks, you define streaming tables and materialized views that the system should create and keep up to date. Delta Live Tables manages how your data is transformed based on queries you define for each processing step

More info on streaming tables, materialized views, and views maintained as the results of declarative queries : [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)

## Architecture

<p align="center">
  <img src="https://github.com/IndraT97/DBX_Delta_live_Table/blob/main/Architecture.png">
</p>

# Databricks Auto Loader

Databricks Autoloader is a feature that automatically loads raw data files into Delta Lake tables in cloud storage locations without additional configuration. It uses Structured Streaming to monitor input directories for new files in various file formats and automatically load them into the tables

As files are discovered, their metadata is persisted in a scalable key-value store in the checkpoint location of your Auto Loader pipeline. This key-value store ensures that data is processed exactly once.

For more information on Autoloader concept : [Databricks Autoloader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
