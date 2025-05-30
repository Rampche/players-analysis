# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Gold Layer - Premier League Teams and Players

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
from functools import reduce
from pyspark.sql import DataFrame, Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Criação das tabelas necessárias para as análises

# COMMAND ----------

# MAGIC %md
# MAGIC # Ranking de jogadores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ranking de jogadores mais valiosos por temporada e no geral

# COMMAND ----------

def create_valuable_players_ranking(silver_path: str, gold_path: str):
    """Cria ranking de jogadores mais valiosos por temporada e geral."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def extract_season_year(season_path):
        return int(season_path.split('_')[1])
    
    try:
        # Lista todos os times e temporadas
        all_data = []
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        
        for team in teams:
            seasons = [s.name.strip('/') for s in dbutils.fs.ls(f"{silver_path}/{team}")]
            
            for season in seasons:
                season_year = extract_season_year(season)
                if season_year >= 2010:
                    df = spark.read.parquet(f"{silver_path}/{team}/{season}")
                    df = df.withColumn("season", lit(season_year))
                    df = df.withColumn("team", lit(team))
                    all_data.append(df)
        
        # União de todos os dados
        complete_df = reduce(DataFrame.unionByName, all_data)
        
        # Ranking por temporada
        season_ranking = (complete_df
            .select("players_name", "team", "season", "players_marketValue")
            .withColumn("rank", row_number().over(
                Window.partitionBy("season")
                .orderBy(desc("players_marketValue"))))
            .filter(col("rank") <= 10))
        
        # Ranking geral
        overall_ranking = (complete_df
            .groupBy("players_name")
            .agg(
                max("players_marketValue").alias("max_value"))
            .withColumn("rank", row_number().over(Window.orderBy(desc("max_value"))))
            .filter(col("rank") <= 15))
        
        # Salvando as tabelas
        (season_ranking
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/season_ranking"))
        
        (overall_ranking
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/overall_ranking"))
        
        logging.info("Tabelas gold criadas com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")

# Execução
create_valuable_players_ranking(
    silver_path="dbfs:/mnt/players-analysis/silver_2/teams",
    gold_path="dbfs:/mnt/players-analysis/gold/valuable_players"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Função para ler as tabelas

# COMMAND ----------

def explore_table(table_dir, table_name):
    table_path = f"dbfs:/mnt/players-analysis/gold/{table_dir}/{table_name}"
    
    table_to_read = spark.read.parquet(table_path)
    return table_to_read

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mais caros de acordo com as temporadas, a partir de 2010

# COMMAND ----------

display(explore_table("valuable_players", "season_ranking"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mais caros entre todas as temporadas

# COMMAND ----------

display(explore_table("valuable_players", "overall_ranking"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlação entre valor de mercado, idade e posição

# COMMAND ----------


def create_market_value_correlations(silver_path: str, gold_path: str):
    """Cria análises de correlação entre valor de mercado, idade e posição."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def extract_season_year(season_path):
        return int(season_path.split('_')[1])
    
    try:
        # Coleta dados
        all_data = []
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        
        for team in teams:
            seasons = [s.name.strip('/') for s in dbutils.fs.ls(f"{silver_path}/{team}")]
            
            for season in seasons:
                season_year = extract_season_year(season)
                if season_year >= 2010:
                    df = spark.read.parquet(f"{silver_path}/{team}/{season}")
                    df = df.withColumn("season", lit(season_year))
                    all_data.append(df)
        
        complete_df = reduce(DataFrame.unionByName, all_data)
        
        # Correlação idade x valor por temporada
        age_correlation = (complete_df
            .groupBy("season")
            .agg(
                corr("players_age", "players_marketValue").alias("age_value_correlation"),
                avg("players_marketValue").alias("avg_market_value"))
            .orderBy("season"))
        
        # Análise por posição
        position_analysis = (complete_df
            .groupBy("season", "players_position")
            .agg(
                avg("players_marketValue").alias("avg_value"),
                count("*").alias("player_count"))
            .orderBy("season", desc("avg_value")))
        
        # Análise detalhada por faixa etária
        age_groups = (complete_df
            .withColumn("age_group", 
                when(col("players_age") < 20, "Under 20")
                .when(col("players_age").between(20, 23), "20-23")
                .when(col("players_age").between(24, 27), "24-27")
                .when(col("players_age").between(28, 31), "28-31")
                .otherwise("Over 31"))
            .groupBy("season", "age_group")
            .agg(
                avg("players_marketValue").alias("avg_value"),
                count("*").alias("player_count"))
            .orderBy("season", "age_group"))
        
        # Salvando as tabelas
        (age_correlation
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/age_correlation"))
        
        (position_analysis
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/position_analysis"))
        
        (age_groups
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/age_groups_analysis"))
        
        logging.info("Análises de correlação criadas com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")

# Execução
create_market_value_correlations(
    silver_path="dbfs:/mnt/players-analysis/silver_2/teams",
    gold_path="dbfs:/mnt/players-analysis/gold/market_correlations"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lesões

# COMMAND ----------

def analyze_injuries(silver_path: str, gold_path: str):
    """Analisa padrões de lesões por time e posição."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def extract_season_year(season_path):
        return int(season_path.split('_')[1])
    
    try:
        # Coleta dados
        all_data = []
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        
        for team in teams:
            seasons = [s.name.strip('/') for s in dbutils.fs.ls(f"{silver_path}/{team}")]
            
            for season in seasons:
                season_year = extract_season_year(season)
                if season_year >= 2010:
                    df = spark.read.parquet(f"{silver_path}/{team}/{season}")
                    df = df.withColumn("season", lit(season_year))
                    df = df.withColumn("team", lit(team))
                    all_data.append(df)
        
        complete_df = reduce(DataFrame.unionByName, all_data)
        
        # Análise de lesões por time
        team_injuries = (complete_df
            .groupBy("season", "team")
            .agg(
                count("*").alias("total_players"),
                sum(when(col("players_status") == "Injuried", 1).otherwise(0)).alias("injured_players"))
            .withColumn("injury_rate", round(col("injured_players") / col("total_players") * 100, 2))
            .orderBy("season", desc("injured_players")))
        
        # Análise de lesões por posição
        position_injuries = (complete_df
            .groupBy("season", "players_position")
            .agg(
                count("*").alias("total_players"),
                sum(when(col("players_status") == "Injuried", 1).otherwise(0)).alias("injured_players"))
            .withColumn("injury_rate", round(col("injured_players") / col("total_players") * 100, 2))
            .orderBy("season", desc("injury_rate")))
        
        # Salvando as tabelas
        (team_injuries
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/team_injuries"))
        
        (position_injuries
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/position_injuries"))
        
        logging.info("Análises de lesões criadas com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")

# Execução
analyze_injuries(
    silver_path="dbfs:/mnt/players-analysis/silver_2/teams",
    gold_path="dbfs:/mnt/players-analysis/gold/injury_analysis"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Lesões por time

# COMMAND ----------

display(explore_table("injury_analysis", "team_injuries"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lesões por posição

# COMMAND ----------

display(explore_table("injury_analysis", "position_injuries"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Média de idade dos elencos

# COMMAND ----------

from pyspark.sql.functions import year, avg, round

def analyze_squad_age_evolution(silver_path: str, gold_path: str):
    """Analisa a evolução da média de idade dos elencos ao longo das temporadas."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def extract_season_year(season_path):
        return int(season_path.split('_')[1])
    
    try:
        # Lista todos os times e temporadas
        all_data = []
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        
        for team in teams:
            seasons = [s.name.strip('/') for s in dbutils.fs.ls(f"{silver_path}/{team}")]
            
            for season in seasons:
                season_year = extract_season_year(season)
                if season_year >= 2005:
                    df = spark.read.parquet(f"{silver_path}/{team}/{season}")
                    df = df.withColumn("season", lit(season_year))
                    df = df.withColumn("team", lit(team))
                    all_data.append(df)
        
        # União de todos os dados
        complete_df = reduce(DataFrame.unionByName, all_data)
        
        # Média de idade por time e temporada
        team_age_evolution = (complete_df
            .groupBy("team", "season")
            .agg(round(avg("players_age"), 2).alias("avg_squad_age"))
            .orderBy("team", "season"))
        
        # Média de idade geral por temporada
        season_age_evolution = (complete_df
            .groupBy("season")
            .agg(round(avg("players_age"), 2).alias("avg_season_age"))
            .orderBy("season"))
        
        # Estatísticas por time
        team_age_stats = (complete_df
            .groupBy("team")
            .agg(
                round(avg("players_age"), 2).alias("overall_avg_age"),
                round(min("players_age"), 2).alias("min_age"),
                round(max("players_age"), 2).alias("max_age")
            )
            .orderBy(desc("overall_avg_age")))
        
        # Salvando as tabelas
        (team_age_evolution
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/team_age_evolution"))
        
        (season_age_evolution
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/season_age_evolution"))
        
        (team_age_stats
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/team_age_stats"))
        
        logging.info("Análise de evolução de idade dos elencos criada com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")

# Execução
analyze_squad_age_evolution(
    silver_path="dbfs:/mnt/players-analysis/silver_2/teams",
    gold_path="dbfs:/mnt/players-analysis/gold/squad_age_analysis"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Evolução da idade por time e temporada

# COMMAND ----------

display(explore_table("squad_age_analysis", "team_age_evolution"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evolução da idade por temporada

# COMMAND ----------

display(explore_table("squad_age_analysis", "season_age_evolution"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estatísticas de idade por time

# COMMAND ----------

display(explore_table("squad_age_analysis", "team_age_stats"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Valor de mercado do time

# COMMAND ----------


def analyze_team_market_evolution(silver_path: str, gold_path: str):
    """Analisa evolução do valor de mercado dos times ao longo do tempo."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def extract_season_year(season_path):
        return int(season_path.split('_')[1])
    
    def calculate_growth_percent(current, previous):
        return round(((current - previous) / previous) * 100, 2)
    
    try:
        # Coleta dados
        all_data = []
        teams = [team.name.strip('/') for team in dbutils.fs.ls(silver_path)]
        
        for team in teams:
            seasons = [s.name.strip('/') for s in dbutils.fs.ls(f"{silver_path}/{team}")]
            
            for season in seasons:
                season_year = extract_season_year(season)
                if season_year >= 2010:
                    df = spark.read.parquet(f"{silver_path}/{team}/{season}")
                    df = df.withColumn("season", lit(season_year))
                    df = df.withColumn("team", lit(team))
                    all_data.append(df)
        
        complete_df = reduce(DataFrame.unionByName, all_data)
        
        # Valor total por time/temporada
        team_values = (complete_df
            .groupBy("season", "team")
            .agg(
                sum("players_marketValue").alias("total_value"),
                count("*").alias("squad_size"))
            .orderBy("team", "season"))
        
        # Cálculo de crescimento
        window_spec = Window.partitionBy("team").orderBy("season")
        
        team_growth = (team_values
            .withColumn("previous_value", lag("total_value").over(window_spec))
            .withColumn("yoy_growth", 
                when(col("previous_value").isNotNull(),
                     (col("total_value") - col("previous_value")) / col("previous_value") * 100)
                .otherwise(None))
            .withColumn("avg_player_value", round(col("total_value") / col("squad_size"), 2)))
        
        # Crescimento total por time
        total_growth = (team_values
            .groupBy("team")
            .agg(
                min("season").alias("start_season"),
                max("season").alias("end_season"),
                first("total_value").alias("initial_value"),
                last("total_value").alias("final_value"))
            .withColumn("total_growth_percent", 
                ((col("final_value") - col("initial_value")) / col("initial_value") * 100))
            .orderBy(desc("total_growth_percent")))
        
        # Salvando as tabelas
        (team_growth
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/yearly_evolution"))
        
        (total_growth
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{gold_path}/total_growth"))
        
        logging.info("Análises de evolução de mercado criadas com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro crítico: {str(e)}")

# Execução
analyze_team_market_evolution(
    silver_path="dbfs:/mnt/players-analysis/silver_2/teams",
    gold_path="dbfs:/mnt/players-analysis/gold/market_evolution"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise do crescimento total por time

# COMMAND ----------

display(explore_table("market_evolution", "total_growth"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlação idade x valor por temporada - Desistência

# COMMAND ----------

display(explore_table("market_correlations", "age_correlation"))

# COMMAND ----------

display(explore_table("market_correlations","age_groups_analysis"))

# COMMAND ----------

display(explore_table("market_correlations", "position_analysis"))

# COMMAND ----------

def normalize_team_name(team_name: str) -> str:
    """Normaliza nomes de times removendo 'FC' e convertendo underlines em espaços."""
    return ' '.join(team_name.replace('FC', '').replace('_', ' ').split())

def analyze_squad_age_evolution(silver_path: str, gold_path: str):
    """Analisa dados dos times por temporada."""
    try:
        teams = [normalize_team_name(team.name.rstrip('/')) 
                for team in dbutils.fs.ls(silver_path)]
        
        seasons_by_team = {team: [s.name.rstrip('/') 
                          for s in dbutils.fs.ls(f"{silver_path}/{team}")] 
                          for team in teams}
        
        return seasons_by_team
        
    except Exception as e:
        logging.error(f"Erro ao processar dados: {str(e)}")