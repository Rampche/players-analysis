# Databricks notebook source
# MAGIC %md
# MAGIC # Staging Layer - Premier League team and players

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import do Dataset

# COMMAND ----------

!pip install kagglehub

import kagglehub

# Download latest version of the 'All Premier League Teams and Players (1992-2024)' Dataset
path = kagglehub.dataset_download("samoilovmikhail/all-premier-league-team-and-players-1992-2024")
dbutils.fs.mv(f"file:{path}", "dbfs:/mnt/players-analysis/staging/", recurse=True)
print("Path to dataset files:", path)

# Configurando as credenciais da AWS no Databricks
spark.conf.set("fs.s3a.access.key", "ACCESS_KEY")
spark.conf.set("fs.s3a.secret.key", "SECRET_KEY")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
 

staging_path = "dbfs:/mnt/players-analysis/staging" 
bronze_path = "dbfs:/mnt/players-analysis/bronze"  
silver_path = "dbfs:/mnt/players-analysis/silver_2/"  
gold_path = "dbfs:/mnt/players-analysis/gold/"     
 
# Definição dos caminhos de destino no Amazon S3
staging_s3_path = "s3a://json-aws-greg-personal-project/staging/" 
bronze_s3_path = "s3a://json-aws-greg-personal-project//bronze/" 
silver_s3_path = "s3a://json-aws-greg-personal-project//silver/" 
gold_s3_path = "s3a://json-aws-greg-personal-project//gold/"     

# Transferir os arquivos da camada Bronze do DBFS para o bucket no S3
print("Transferring Bronze files to S3...")
dbutils.fs.cp(staging_path, staging_s3_path, recurse=True)
print("Bronze files transferred successfully!")

# Transferir os arquivos da camada Bronze do DBFS para o bucket no S3
print("Transferring Bronze files to S3...")
dbutils.fs.cp(bronze_path, bronze_s3_path, recurse=True)
print("Bronze files transferred successfully!")
 
# Transferir os arquivos da camada Silver do DBFS para o bucket no S3
print("Transferring Silver files to S3...")
dbutils.fs.cp(silver_path, silver_s3_path, recurse=True)
print("Silver files transferred successfully!")
 
# Transferir os arquivos da camada Gold do DBFS para o bucket no S3
print("Transferring Gold files to S3...")
dbutils.fs.cp(gold_path, gold_s3_path, recurse=True)
print("Gold files transferred successfully!")
 
spark.conf.unset("fs.s3a.access.key")
spark.conf.unset("fs.s3a.secret.key")
spark.conf.unset("fs.s3a.endpoint")
 
# Mensagem final indicando que todo o processo foi concluído
print("All files have been successfully transferred to S3!")