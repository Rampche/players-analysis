# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Premier League team and players

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports necessários

# COMMAND ----------

import json
import logging
import os
import re
from typing import Dict, Optional, Union
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import pandas as pd
import pyarrow.parquet as pq

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/players-analysis/silver_2", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversão para o formato parquet

# COMMAND ----------

bronze_base_path = "dbfs:/mnt/players-analysis/bronze/teams"

def process_team_season(spark, bronze_path: str, silver_path: str, team_name: str, season: str) -> dict:
    """
    Processa os dados de um time em uma temporada específica, convertendo de JSON para Parquet.
    
    """
    # Constrói os caminhos de origem e destino
    source_path = f"{bronze_path}/{team_name}/Season_{season}"
    target_path = f"{silver_path}/{team_name}/Season_{season}"
    
    try:

        # Define o schema do JSON
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("players", ArrayType(
                StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("position", StringType(), True),
                    StructField("dateOfBirth", StringType(), True),
                    StructField("age", StringType(), True),
                    StructField("nationality", ArrayType(StringType()), True),
                    StructField("currentClub", StringType(), True),
                    StructField("height", StringType(), True),
                    StructField("foot", StringType(), True),
                    StructField("joinedOn", StringType(), True),
                    StructField("signedFrom", StringType(), True),
                    StructField("marketValue", StringType(), True),
                    StructField("status", StringType(), True)
                ])
            ), True),
            StructField("updatedAt", StringType(), True)
        ])

        # Lê o arquivo JSON usando o schema definido
        df = spark.read.json(f"{source_path}/*.json", schema=schema)

        # Explode o array de jogadores
        df_exploded = df.select("id", "updatedAt", explode("players").alias("player"))

        # Cria as colunas com o prefixo "players_"
        df_players_columns = df_exploded.select(
            "id",
            "updatedAt",
            col("player.id").alias("players_id"),
            col("player.name").alias("players_name"),
            col("player.position").alias("players_position"),
            col("player.dateOfBirth").alias("players_dateOfBirth"),
            col("player.age").alias("players_age"),
            col("player.nationality").alias("players_nationality"),
            col("player.currentClub").alias("players_currentClub"),
            col("player.height").alias("players_height"),
            col("player.foot").alias("players_foot"),
            col("player.joinedOn").alias("players_joinedOn"),
            col("player.signedFrom").alias("players_signedFrom"),
            col("player.marketValue").alias("players_marketValue"),
            col("player.status").alias("players_status")
        )
 
        # Otimiza o DataFrame para escrita
        df_formated_players = df_formated_players.repartition(1)  # Uma partição é suficiente pois são dados pequenos por temporada
        
        # Salva o DataFrame como Parquet
        df_players_columns.write.mode("overwrite").parquet(target_path)

        return {
            "status": "success",
            "team": team_name,
            "season": season,
            "record_count": df.count()
        }
    except Exception as e:
        return {
            "status": "error",
            "team": team_name,
            "season": season,
            "error": str(e)
        }

def convert_json_to_parquet():
    """
    Função principal que coordena a conversão de todos os arquivos JSON para Parquet.
    """
    # Define os caminhos base
    bronze_path = "dbfs:/mnt/players-analysis/bronze/teams"
    silver_path = "dbfs:/mnt/players-analysis/silver_2/teams"
    
    # Lista todos os times na camada bronze
    teams = [team.name.strip('/') for team in dbutils.fs.ls(bronze_path)]
    
    # Estatísticas do processamento
    processing_stats = {
        "successful_conversions": 0,
        "failed_conversions": 0,
        "processed_teams": 0,
        "total_records": 0,
        "details": []
    }
    
    # Processa cada time
    for team in teams:
        print(f"\nProcessando time: {team}")
        
        # Lista todas as temporadas do time
        seasons_path = f"{bronze_path}/{team}"
        seasons = [season.name.strip('/').split('_')[1] 
                  for season in dbutils.fs.ls(seasons_path)]
        
        # Processa cada temporada
        for season in seasons:
            result = process_team_season(spark, bronze_path, silver_path, team, season)
            processing_stats["details"].append(result)
            
            if result["status"] == "success":
                processing_stats["successful_conversions"] += 1
                processing_stats["total_records"] += result["record_count"]
                print(f"  ✓ Temporada {season}: {result['record_count']} registros")
            else:
                processing_stats["failed_conversions"] += 1
                print(f"  ✗ Temporada {season}: ERRO - {result['error']}")
        
        processing_stats["processed_teams"] += 1
    
    # Imprime relatório final
    print("\nRelatório de Conversão:")
    print(f"Times processados: {processing_stats['processed_teams']}")
    print(f"Conversões bem-sucedidas: {processing_stats['successful_conversions']}")
    print(f"Conversões com erro: {processing_stats['failed_conversions']}")
    print(f"Total de registros processados: {processing_stats['total_records']}")
    
    return processing_stats

# Executa a conversão
stats = convert_json_to_parquet()

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploraração dos arquivos parquet 

# COMMAND ----------

## Conferência para fins Data Quality

# COMMAND ----------

def explore_parquet_data(team_name, season):
    
    # Caminho para os dados do time específico
    parquet_path = f"dbfs:/mnt/players-analysis/silver_2/teams/{team_name}/Season_{season}"
    
    df_team = spark.read.parquet(parquet_path)
    return df_team

# COMMAND ----------

# Lendo valores do arquivo do Arsenal, na temporada de 2020.
arsenal_2020 = explore_parquet_data("Arsenal_FC", "2020")
display(arsenal_2020)

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpezas nos tipos dos dados

# COMMAND ----------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# UDFs
@udf(IntegerType())
def clean_market_value(value):
    if not value or value == "0":
        return 0
    value = value.replace("€", "").strip().lower()
    multiplier = 1000 if "k" in value else 1000000 if "m" in value else 1
    return int(float(value.replace("k", "").replace("m", "")) * multiplier)

@udf(StringType())
def clean_club_name(name):
    if not name:
        return "No team"
    cleaned_name = name.split(":")[0].strip()
    return "No team" if not cleaned_name else cleaned_name

@udf(StringType())
def clean_status(status):
    if not status:
        return "Available to play"
    
    status = status.lower()
    if status == "team captain":
        return "Available to play"
    elif "team captain" in status:
        return "Injuried"
    return "Injuried"

@udf(StringType())
def capitalize_first_letter(text):
    if not text:
        return None
    return text.capitalize()

def calculate_partitions(record_count: int) -> int:
    """Calcula o número ideal de partições baseado no volume de dados."""
    if record_count <= 10000:
        return 1
    elif record_count <= 40000:
        return 2
    elif record_count <= 80000:
        return 3
    else:
        return 4

def transform_dataframe(df):
    """Aplica todas as transformações no DataFrame."""
    return (df
        .drop("id", "updatedAt", "players_id", "players_dateOfBirth", "players_joinedOn")
        .withColumns({
            "players_age": col("players_age").cast("int"),
            "player_first_nationality": coalesce(element_at(col("players_nationality"), 1), lit("None")),
            "player_second_nationality": coalesce(element_at(col("players_nationality"), 2), lit("None")),
            "players_height": regexp_replace(col("players_height"), ",", "").substr(1, 3).cast("int"),
            "players_marketValue": clean_market_value(col("players_marketValue")),
            "players_status": coalesce(col("players_status"), lit("available to play")),
            "players_signedFrom": clean_club_name(col("players_signedFrom"))
        })
        .drop("players_nationality"))

def process_season(spark, source_path, target_path):
    """Processa uma temporada específica."""
    try:
        df = spark.read.parquet(source_path).cache()
        record_count = df.count()
        
        # Calcula número de partições
        num_partitions = calculate_partitions(record_count)
        
        (transform_dataframe(df)
         .repartition(num_partitions)
         .write
         .mode("overwrite")
         .option("compression", "snappy")
         .parquet(target_path))
        
        df.unpersist()
        return {"status": "success", "record_count": record_count}
    
    except Exception as e:
        logging.error(f"Erro: {str(e)}")
        return {"status": "error", "error": str(e)}

def normalize_data(silver_path: str, updated_silver_path: str):
    """Função principal de normalização."""
    stats = {"successes": 0, "failures": 0, "total_records": 0}
    
    try:
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        
        for team in teams:
            logging.info(f"Processando: {team}")
            seasons = [s.name.strip('/').split('_')[1] for s in dbutils.fs.ls(f"{silver_path}/{team}")]
            
            for season in seasons:
                result = process_season(
                    spark,
                    f"{silver_path}/{team}/Season_{season}",
                    f"{updated_silver_path}/{team}/Season_{season}"
                )
                
                if result["status"] == "success":
                    stats["successes"] += 1
                    stats["total_records"] += result["record_count"]
                    logging.info(f"✓ {team} - Season_{season}: {result['record_count']} registros")
                else:
                    stats["failures"] += 1
                    logging.error(f"✗ {team} - Season_{season}: {result['error']}")
        
        logging.info(f"""
        Normalização concluída:
        Sucessos: {stats['successes']}
        Falhas: {stats['failures']}
        Registros: {stats['total_records']}
        """)
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")
    
    return stats

# Execução
stats = normalize_data(
    silver_path="dbfs:/mnt/players-analysis/silver/teams",
    updated_silver_path="dbfs:/mnt/players-analysis/silver_2/teams"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Coreção apenas das colunas Player Status e Players Foot

# COMMAND ----------

@udf(StringType())
def clean_status(status):
    if not status:
        return "Available to play"
    
    status = status.lower()
    if status == "team captain" or status == "available to play":
        return "Available to play"
    elif "team captain" in status:
        return "Injuried"
    return "Injuried"

def fix_status_and_foot(silver_path: str):
    """Corrige status e foot em todos os arquivos."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    @udf(StringType())
    def clean_status(status):
        if not status:
            return "Available to play"
        
        status = status.lower()
        if status == "team captain" or status == "available to play":
            return "Available to play"
        elif "team captain" in status:
            return "Injuried"
        return "Injuried"

    @udf(StringType())
    def capitalize_first_letter(text):
        if not text:
            return None
        return text.capitalize()

    try:
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        
        for team in teams:
            logging.info(f"Processando time: {team}")
            seasons = [s.name.strip('/') for s in dbutils.fs.ls(f"{silver_path}/{team}")]
            
            for season in seasons:
                logging.info(f"Processando: {team}/{season}")
                
                # Lê o arquivo
                df = spark.read.parquet(f"{silver_path}/{team}/{season}")
                
                # Aplica as correções
                updated_df = df.withColumns({
                    "players_status": capitalize_first_letter(clean_status(col("players_status"))),
                    "players_foot": capitalize_first_letter(col("players_foot"))
                })
                
                # Sobrescreve o arquivo original
                updated_df.write.mode("overwrite").option("compression", "snappy").parquet(f"{silver_path}/{team}/{season}")
                
                logging.info(f"✓ Concluído: {team}/{season}")
                
        logging.info("Processo de correção finalizado com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")

# Execução
fix_status_and_foot("dbfs:/mnt/players-analysis/silver_2/teams")


# COMMAND ----------

arsenal_2020_2 = explore_parquet_data_2("Arsenal_FC", "2020")
display(arsenal_2020_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correção dos valores vazios e nulos na coluna "players_signedFrom"

# COMMAND ----------

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')

def fix_signed_from(silver2_path: str):
    """Corrige valores nulos/vazios na coluna players_signedFrom para 'None'."""
    stats = {"success": 0, "errors": 0}
    
    try:
        for team in [t.name.strip('/') for t in dbutils.fs.ls(silver2_path)]:
            for season in [s.name.strip('/').split('_')[1] for s in dbutils.fs.ls(f"{silver2_path}/{team}")]:
                path = f"{silver2_path}/{team}/Season_{season}"
                
                try:
                    (spark.read.parquet(path)
                     .withColumn(
                         "players_signedFrom",
                         coalesce(
                             when(col("players_signedFrom").isNull() | 
                                 (col("players_signedFrom") == ""), lit("None"))
                             .otherwise(col("players_signedFrom")),
                             lit("None")
                         )
                     )
                     .write.mode("overwrite").parquet(path))
                    
                    stats["success"] += 1
                    logging.info(f"✓ {team} - Season_{season}")
                    
                except Exception as e:
                    stats["errors"] += 1
                    logging.error(f"✗ {team} - Season_{season}: {str(e)}")
        
        logging.info(f"Concluído - Sucessos: {stats['success']}, Erros: {stats['errors']}")
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")
    
    return stats

# Execução
stats = fix_signed_from("dbfs:/mnt/players-analysis/silver_2/teams")


# COMMAND ----------

arsenal_2022 = explore_parquet_data_2("Arsenal_FC", "2022")
display(arsenal_2022)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura do arquivo do Manchester City na temporada de 2024

# COMMAND ----------

df_man_city_2024 = explore_parquet_data_2("Manchester_City", "2024")
display(df_man_city_2024)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Código para limpar outras sujeiras existentes

# COMMAND ----------

def clean_player_data(silver_path: str):
    """Remove dados inválidos ou desnecessários dos arquivos."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        
        for team in teams:
            logging.info(f"Processando time: {team}")
            seasons = [s.name.strip('/') for s in dbutils.fs.ls(f"{silver_path}/{team}")]
            
            for season in seasons:
                logging.info(f"Processando: {team}/{season}")
                
                # Lê o arquivo
                df = spark.read.parquet(f"{silver_path}/{team}/{season}")
                
                # Aplica as limpezas
                cleaned_df = (df
                    .drop("players_currentClub")
                    .filter(col("players_height").isNotNull() & (col("players_height") > 0))
                    .filter(col("players_marketValue").isNotNull() & (col("players_marketValue") > 0))
                    .filter(col("players_foot").isNotNull()))
                
                # Sobrescreve o arquivo original
                cleaned_df.write.mode("overwrite").option("compression", "snappy").parquet(f"{silver_path}/{team}/{season}")
                
                logging.info(f"✓ Concluído: {team}/{season}")
        
        logging.info("Processo de limpeza finalizado com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")

# Execução
clean_player_data("dbfs:/mnt/players-analysis/silver_2/teams")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Testes para verificar se a qualidade dos dados está da forma esperada.

# COMMAND ----------

df_portsmouth_2009 = explore_parquet_data_2("Portsmouth_FC", "2009")
display(df_portsmouth_2009)

# COMMAND ----------

def rename_team_folders(silver_path: str):
    """Renomeia as pastas dos times removendo FC e formatando os nomes."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        for team in teams:
            # Remove FC e formata o nome
            new_name = team.replace('_FC', '').replace('FC_', '').replace('FC', '')
            new_name = new_name.replace('_', ' ').strip()
            
            if team != new_name:
                old_path = f"{silver_path}/{team}"
                new_path = f"{silver_path}/{new_name}"
                dbutils.fs.mv(old_path, new_path)
                logging.info(f"Renomeado: {team} -> {new_name}")
                
        logging.info("Processo de renomeação finalizado com sucesso!")
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")

# Execução
rename_team_folders("dbfs:/mnt/players-analysis/silver_2/teams")