# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Premier League team and players

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reorganização dos dados

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col
import re
from typing import Dict, List, Set
from collections import defaultdict

def extract_team_name(filename: str) -> str:
    """
    Extrai o nome do time do arquivo, considerando que o nome termina antes do primeiro número.
    Exemplo: 'Arsenal_FC_11_2010.json' -> 'Arsenal_FC'
    
    Args:
        filename: Nome do arquivo a ser processado
    Returns:
        Nome do time extraído
    """
    return re.split(r'_\d', filename)[0]

def get_all_seasons(base_path: str) -> Set[str]:
    """
    Obtém todas as temporadas disponíveis no dataset.
    
    Args:
        base_path: Caminho base onde estão os arquivos
    Returns:
        Conjunto com todos os anos das temporadas
    """
    seasons = dbutils.fs.ls(base_path)
    return {season_dir.path.split('_')[-1].strip('/') for season_dir in seasons}

def validate_team_seasons(team_files: Dict[str, Dict[str, dict]], all_seasons: Set[str]) -> Dict[str, Set[str]]:
    """
    Identifica temporadas faltantes para cada time.
    
    Args:
        team_files: Dicionário com arquivos organizados por time e temporada
        all_seasons: Conjunto com todas as temporadas disponíveis
    Returns:
        Dicionário com as temporadas em que cada time participou da Premier League
    """
    team_seasons = defaultdict(set)
    
    # Para cada temporada, verifica quais times têm arquivos
    for season_dir in dbutils.fs.ls(staging_base_path):
        season_year = season_dir.path.split('_')[-1].strip('/')
        
        # Lista todos os arquivos da temporada
        season_files = dbutils.fs.ls(season_dir.path)
        
        # Registra cada time presente nesta temporada
        for file in season_files:
            team_name = extract_team_name(file.name)
            team_seasons[team_name].add(season_year)
    
    return team_seasons

def reorganize_premier_league_files():
    """
    Reorganiza os arquivos JSON da camada staging para a bronze, estruturando por time e temporada,
    garantindo que cada time tenha todos os seus elencos históricos preservados.
    """
    # Path base para as camadas staging e bronze
    global staging_base_path
    staging_base_path = "dbfs:/mnt/players-analysis/staging/DATA_JSON"
    bronze_base_path = "dbfs:/mnt/players-analysis/bronze/teams"
    
    # Obtém todas as temporadas disponíveis
    all_seasons = get_all_seasons(staging_base_path)
    
    # Dicionário para armazenar arquivos por time
    team_files = defaultdict(dict)
    
    # Primeiro passo: mapear todos os arquivos disponíveis
    for season_dir in dbutils.fs.ls(staging_base_path):
        season_path = season_dir.path
        season_year = season_path.split('_')[-1].strip('/')
        
        # Lista todos os arquivos JSON da temporada
        json_files = dbutils.fs.ls(season_path)
        
        for json_file in json_files:
            filename = json_file.name
            team_name = extract_team_name(filename)
            
            # Adiciona arquivo à estrutura do time
            team_files[team_name][season_year] = {
                'source_path': json_file.path,
                'filename': filename
            }
    
    # Valida as temporadas de cada time
    team_seasons = validate_team_seasons(team_files, all_seasons)
    
    # Cria nova estrutura na camada silver
    missing_files = []
    processed_files = []
    
    for team_name, valid_seasons in team_seasons.items():
        # Cria diretório base do time
        team_base_path = f"{bronze_base_path}/{team_name}"
        dbutils.fs.mkdirs(team_base_path)
        
        # Processa cada temporada que o time participou
        for season in valid_seasons:
            if season in team_files[team_name]:
                season_path = f"{team_base_path}/Season_{season}"
                dbutils.fs.mkdirs(season_path)
                
                file_info = team_files[team_name][season]
                destination_path = f"{season_path}/{file_info['filename']}"
                
                # Copia arquivo para nova localização
                dbutils.fs.cp(file_info['source_path'], destination_path)
                processed_files.append(f"{team_name}/Season_{season}")
            else:
                missing_files.append(f"{team_name}/Season_{season}")
    
    # Gera relatório da reorganização
    report = {
        'total_teams': len(team_seasons),
        'teams': list(team_seasons.keys()),
        'processed_files': len(processed_files),
        'missing_files': len(missing_files),
        'seasons_by_team': {team: sorted(list(seasons)) 
                           for team, seasons in team_seasons.items()},
        'structure_example': f"{bronze_base_path}/Arsenal_FC/Season_2010/Arsenal_FC_11_2010.json"
    }
    
    print("\nReorganização concluída!")
    print(f"\nTotal de times processados: {report['total_teams']}")
    print(f"Total de arquivos processados: {report['processed_files']}")
    print(f"Total de arquivos faltantes: {report['missing_files']}")
    
    # Mostra alguns exemplos de temporadas por time
    print("\nExemplo de temporadas por time:")
    for team, seasons in list(report['seasons_by_team'].items())[:3]:
        print(f"{team}: {', '.join(seasons)}")
    
    return report

# Executa a reorganização
report = reorganize_premier_league_files()