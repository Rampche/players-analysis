# Premier League Teams and Players Data Analysis

This project performs an end-to-end ETL (Extract, Transform, Load) process on Premier League team and player data, leveraging Databricks and Spark for data processing and analysis. The data is structured into a medallion architecture (Staging, Bronze, Silver, Gold layers) to ensure data quality, consistency, and analytical readiness.

## Project Structure

The project is organized into four main layers, each represented by a Python notebook designed to run on Databricks:

1.  **`1. Staging Layer.py`**: Handles data extraction from Kaggle and initial loading into DBFS, followed by transfer to an S3 bucket.
2.  **`2. Bronze Layer.py`**: Focuses on reorganizing the raw JSON data into a more structured format, grouped by team and season.
3.  **`3. Silver Layer.py`**: Performs data cleaning, type conversions, and normalization, transforming JSON data into Parquet format for optimized querying.
4.  **`4. Gold Layer.py`**: Generates aggregated and enriched datasets for various analytical purposes, storing them in Parquet format.

## Data Layers Explained

### Staging Layer (`1. Staging Layer.py`)

* **Purpose**: Initial data ingestion and raw storage.
* **Process**:
    * Downloads the 'All Premier League Teams and Players (1992-2024)' dataset from Kaggle Hub.
    * Moves the downloaded files to a staging directory in DBFS (`dbfs:/mnt/players-analysis/staging`).
    * Configures AWS S3 credentials for data transfer.
    * Transfers data from DBFS staging, bronze, silver, and gold paths to corresponding S3 buckets.

### Bronze Layer (`2. Bronze Layer.py`)

* **Purpose**: Raw data storage with initial organization.
* **Process**:
    * Reads raw JSON files from the staging layer.
    * Reorganizes the data into a hierarchical structure: `bronze/teams/{TeamName}/Season_{Year}/`.
    * Extracts team names and season years from filenames.
    * Validates and identifies seasons for each team to ensure data completeness.
    * Copies files to the new bronze layer structure.
    * Generates a report on processed and missing files.

### Silver Layer (`3. Silver Layer.py`)

* **Purpose**: Cleaned, structured, and conformed data.
* **Process**:
    * Converts JSON files from the Bronze layer into Parquet format for improved query performance.
    * Defines a detailed Spark schema for the player data.
    * Explodes nested JSON structures (players array) into flat columns.
    * Performs extensive data cleaning and transformation:
        * Casts `players_age` to integer.
        * Extracts first and second nationalities.
        * Cleans and converts `players_height` to integer.
        * Cleans and converts `players_marketValue` to integer (handling 'k' for thousands and 'm' for millions).
        * Normalizes `players_status` (e.g., "team captain" to "Available to play", others to "Injuried").
        * Cleans `players_signedFrom` to remove extra information.
        * Capitalizes first letters of `players_status` and `players_foot`.
        * Handles null/empty values in `players_signedFrom` by setting them to "None".
        * Removes invalid or unnecessary data (e.g., `players_currentClub`, records with null/zero height or market value, null foot).
    * Renames team folders to remove "FC" and format names consistently (e.g., "Arsenal FC" becomes "Arsenal").
    * Optimizes DataFrame partitioning for writing to Parquet.

### Gold Layer (`4. Gold Layer.py`)

* **Purpose**: Curated, aggregated, and highly optimized data for reporting and analytics.
* **Process**:
    * **Valuable Players Ranking**:
        * Creates a ranking of the most valuable players by season (from 2010 onwards) and an overall ranking.
        * Stores results in `gold/valuable_players/season_ranking` and `gold/valuable_players/overall_ranking`.
    * **Market Value Correlations**:
        * Analyzes the correlation between market value, player age, and position.
        * Generates tables for:
            * Age vs. Market Value correlation by season (`gold/market_correlations/age_correlation`).
            * Market value analysis by player position (`gold/market_correlations/position_analysis`).
            * Market value analysis by age groups (`gold/market_correlations/age_groups_analysis`).
    * **Injury Analysis**:
        * Analyses injury patterns by team and player position.
        * Calculates injury rates.
        * Stores results in `gold/injury_analysis/team_injuries` and `gold/injury_analysis/position_injuries`.
    * **Squad Age Evolution**:
        * Analyzes the evolution of average squad age over seasons.
        * Generates tables for:
            * Average age by team and season (`gold/squad_age_analysis/team_age_evolution`).
            * Overall average age by season (`gold/squad_age_analysis/season_age_evolution`).
            * Age statistics by team (`gold/squad_age_analysis/team_age_stats`).
    * **Team Market Evolution**:
        * Analyses evolution of total team market value over time.
        * Calculates year-over-year growth and average player value.
        * Stores results in `gold/market_evolution/yearly_evolution` and `gold/market_evolution/total_growth`.
    * Includes helper functions to read and explore the generated Gold tables.

## Setup and Execution

This project is designed to run on **Databricks**.

### Prerequisites

* A Databricks workspace.
* Access to a Databricks cluster (Spark 3.x recommended).
* An AWS S3 bucket for storing processed data.
* Kaggle API credentials (for `1. Staging Layer.py` to download the dataset).
* AWS credentials (Access Key and Secret Key) for S3 access.

### Configuration

1.  **AWS Credentials**: In `1. Staging Layer.py`, replace `"ACCESS_KEY"` and `"SECRET_KEY"` with your actual AWS S3 access key and secret key.
    ```python
    spark.conf.set("fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY")
    spark.conf.set("fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY")
    ```
2.  **S3 Paths**: Update the S3 bucket paths in `1. Staging Layer.py` to match your S3 bucket name.
    ```python
    staging_s3_path = "s3a://your-s3-bucket-name/staging/"
    bronze_s3_path = "s3a://your-s3-bucket-name/bronze/"
    silver_s3_path = "s3a://your-s3-bucket-name/silver/"
    gold_s3_path = "s3a://your-s3-bucket-name/gold/"
    ```

### Running the Project

The notebooks should be executed sequentially, as each layer depends on the output of the previous one.

1.  **Upload Notebooks**: Upload all four Python files (`.py`) to your Databricks workspace.
2.  **Attach to Cluster**: Attach each notebook to a running Databricks cluster.
3.  **Run in Order**: Execute the notebooks in the following order:
    1.  `1. Staging Layer.py`
    2.  `2. Bronze Layer.py`
    3.  `3. Silver Layer.py`
    4.  `4. Gold Layer.py`

Each notebook contains `dbutils.fs.rm` commands at the beginning of the Silver layer to clear previous runs, ensuring a clean execution if you re-run the process.

## Analysis and Insights (Gold Layer)

The Gold layer generates several analytical tables that can be used for various insights:

* **Most Valuable Players**: Identify top players by market value per season and overall.
* **Market Value Dynamics**: Understand how market value correlates with age and position over time.
* **Injury Trends**: Analyze injury rates across teams and positions to identify potential risk areas.
* **Squad Age Trends**: Track the average age of squads and individual teams over different seasons.
* **Team Market Evolution**: Observe the growth or decline in a team's total market value year-over-year.

These tables serve as a foundation for building dashboards, reports, or further machine learning models.

## Technologies Used

* **Databricks**: Cloud-based data engineering and machine learning platform.
* **Apache Spark**: Distributed processing engine for large-scale data.
* **Python**: Programming language for scripting and data manipulation.
* **Kaggle Hub**: Dataset source.
* **AWS S3**: Cloud storage for data persistence.
* **Parquet**: Columnar data format for optimized analytical queries.

## Author
Gregório Rampche
