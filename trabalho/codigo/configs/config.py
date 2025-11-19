from pathlib import Path
import os, sys

ROOT = Path.cwd()  # apanha a directoria do projecto 

DIR_CONFIGS = ROOT / "configs"
DIR_SOURCES = ROOT / "sources"
DIR_SCRIPTS = ROOT / "scripts"
DIR_OUTPUT = ROOT / "output"

FILENAME_SOURCES_TO_IMPORT = DIR_SOURCES / "sources.json"
FILENAME_MONGODB_CONFIG = DIR_CONFIGS / "mongodb.py"
FILENAME_CONFIG = DIR_CONFIGS / "config.py"
FILENAME_SCRIPTS_FILES = DIR_SCRIPTS / "files.py"
FILENAME_SENTIMENTOS = DIR_OUTPUT / "sentimentos.csv"

DATABASE_TABLENAME_FILE_CSV = "file_csv_comentarios"
DATABASE_TABLENAME_FEEDS_RSS = "feeds_rss"
DATABASE_TABLENAME_WEBSITE_FEEDBACK = "website_feedback"
DATABASE_TABLENAME_VENDAS = "venda"

DATABASE_BATCH_SIZE_FOR_INSERT = 100

SENTIMENT_MODEL_USED = "cardiffnlp/twitter-xlm-roberta-base-sentiment"