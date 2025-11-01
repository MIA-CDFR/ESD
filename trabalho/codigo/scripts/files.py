# script_files.py
import os
import pandas as pd
import json
import feedparser
import time
from typing import Dict, Any
from configs import config as config

def load_csv(path: str) -> pd.DataFrame:
    """Carrega um ficheiro CSV. Retorna DataFrame vazio se não existir."""
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")
    
def save_csv(path: str, df: pd.DataFrame) -> None:
    """Grava o DataFrame no CSV (UTF-8)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")

def guardar(df):
    df.to_csv(config.FILENAME_CONTACTOS, index=False, encoding="utf-8")

def load_dataFrom_JSON(file_path: str):
# def load_data_from_json(file_path):
    """Carrega os dados de configuração do ficheiro JSON."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"ERRO: Ficheiro não encontrado em '{file_path}'.")
        
    # print(f"A carregar configurações do ficheiro: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def fetch_rss_feed(feed_info: Dict[str, Any]) -> list:
    """Descarrega artigos de um feed RSS e formata para documentos MongoDB."""
    url = feed_info.get("url")
    nome = feed_info.get("nome", "Desconhecido")
    articles = []

    if not url:
        return []
    
    try:
        d = feedparser.parse(url)
        
        for entry in d.entries:
            article = {
                "fonte_nome": nome,
                "fonte_url": url,
                "idioma": feed_info.get("idioma", "N/A"),
                "titulo": entry.get("title", "N/A"),
                "link": entry.get("link", "N/A"),
                "sumario": entry.get("summary", entry.get("description", "N/A")),
                "data_publicacao": entry.get("published_parsed") # Usa a data parseada (UTC)
            }
            articles.append(article)
            
    except Exception as e:
        print(f"ERRO ao descarregar feed '{nome}': {e}")
            
    return articles

