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

## 👩🏻‍💻 Usage Instructions

To explore the contents of this repository:

1. **Clone the repository**:

    ```sh
    git clone https://github.com/IndraT97/DBX_Delta_live_Table.git
    ```

2. **Navigate through the directories** to find case studies, platform solutions, or projects of interest.

3. **Review the SQL queries and accompanying documentation** to understand the problem-solving approaches and methodologies.


## ✏️ Contribution Guidelines

Contributions to this repository are welcome. 🚀

If you have suggestions for improvements, additional case studies, solutions, or projects, please follow these steps:

1. Fork the repository.

2. Create a new branch (`git checkout -b feature-branch`).

3. Commit your changes with detailed messages (`git commit -m 'Add detailed feature description'`).

4. Push to the branch (`git push origin feature-branch`).

5. Submit a pull request for review.

## Support

Do ⭐ the repository, if it inspired you, gave you ideas of your own or helped you in any way !!

I hope you find these resources informative and useful for your SQL learning and application. Should you have any questions or feedback, feel free to reach out to me on [LinkedIn](https://www.linkedin.com/in/i97/). 🙌
