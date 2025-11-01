# -*- coding: utf-8 -*-
"""
Cria/repõe o esquema ESD/Wine em PostgreSQL via Python.
Uso:
    python setup_db.py
Config:
    Define as variáveis de ambiente abaixo ou ajusta os defaults no código.
"""

import os
import sys
import psycopg2
from contextlib import closing
import database

# ----------------------------
# 1) Config da ligação (ENV ou defaults)
# ----------------------------

DADOS_DB_DATABASE = database.DB_CONFIG_DATABASE
PG_DB_DATABASE   = os.getenv("PGDATABASE", DADOS_DB_DATABASE['dbname'])

DADOS_DB = database.DB_CONFIG
PG_HOST = os.getenv("PGHOST", DADOS_DB['host'])
PG_PORT = int(os.getenv("PGPORT", DADOS_DB['port']))
PG_DB   = os.getenv("PGDATABASE", DADOS_DB['dbname'])
PG_USER = os.getenv("PGUSER", DADOS_DB['user'])
PG_PASS = os.getenv("PGPASSWORD", DADOS_DB['password'])

# ----------------------------
# 2) SQL do esquema (PostgreSQL)
# ----------------------------
SQL_SCHEMA_DATABASE = r"""
CREATE DATABASE esd_wine
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Portuguese_Portugal.1252'
    LC_CTYPE = 'Portuguese_Portugal.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False
"""

SQL_SCHEMA = r"""
-- =========================================================
-- ESD / WINE — ESQUEMA COMPLETO (PostgreSQL)
-- =========================================================

-- Drop em ordem para evitar FKs/dep. (é ambiente de dev/demo)
DROP TABLE IF EXISTS users                              CASCADE;
DROP TABLE IF EXISTS feeds_rss                          CASCADE;
DROP TABLE IF EXISTS feeds_rss_processada               CASCADE;
DROP TABLE IF EXISTS file_csv_comentarios               CASCADE;
DROP TABLE IF EXISTS file_csv_comentarios_processada    CASCADE;
DROP TABLE IF EXISTS website_feedback                   CASCADE;
DROP TABLE IF EXISTS website_feedback_processada        CASCADE;
DROP TABLE IF EXISTS vendas                             CASCADE;
DROP TABLE IF EXISTS vendas_processada                  CASCADE;
DROP TABLE IF EXISTS sentimentos                        CASCADE;

-- USERS
CREATE TABLE users (
    userid                  SERIAL PRIMARY KEY,
    nome                    TEXT,
    email                   TEXT
);

INSERT INTO users (nome, email) VALUES
  ('Carlos','ccj.gmr@gmail.com'),
  ('Rui','rmmmrodrigues@gmail.com'),
  ('Filipa','filipapereira306@gmail.com'),
  ('Diego','diegojeffersonms@gmail.com');

-- FEEDS
CREATE TABLE feeds_rss (
    id                      SERIAL PRIMARY KEY,
    fonte_nome              TEXT,
    fonte_url               TEXT,
    idioma                  TEXT,
    titulo                  TEXT,
    link                    TEXT,
    sumario                 TEXT,
    datahora                TIMESTAMP WITHOUT TIME ZONE,    --- = data_publicacao
    extracted_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed               BOOLEAN DEFAULT FALSE
);

CREATE TABLE feeds_rss_processada (
    id                      SERIAL PRIMARY KEY,
    r_id                    INTEGER,
    titulo                  TEXT,
    sumario                 TEXT,
    processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    applied_tas             BOOLEAN DEFAULT FALSE
);

-- COMENTÁRIOS (CSV)
CREATE TABLE file_csv_comentarios (
    id                      SERIAL PRIMARY KEY,
    commentid               INTEGER,
    userid                  INTEGER,
    productid               INTEGER,
    storeid                 INTEGER,
    datahora                TIMESTAMP WITHOUT TIME ZONE,
    texto                   TEXT,
    extracted_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed               BOOLEAN DEFAULT FALSE
);

CREATE TABLE file_csv_comentarios_processada (
    id                      SERIAL PRIMARY KEY,
    r_id                    INTEGER,
    texto                   TEXT,
    processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    applied_tas             BOOLEAN DEFAULT FALSE
);

-- WEBSITE FEEDBACK
CREATE TABLE website_feedback (
    id                      SERIAL PRIMARY KEY,
    object_id               TEXT,
    email                   TEXT,
    comentarios             TEXT,
    classificacao           INTEGER,
    datahora                TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,      --- = ingested_on
    source                  TEXT DEFAULT 'website feedback',
    extracted               BOOLEAN DEFAULT FALSE
);

INSERT INTO website_feedback (object_id, email, comentarios, classificacao, source) VALUES
  ('object id no. 00001','ccj.gmr@gmail.com', 'Adoro vinho do Porto!', '7', 'website feedback'),
  ('object id no. 00002','rmmmrodrigues@gmail.com', 'Fraca experiência com Mateus Rosé', '2', 'website feedback'),
  ('object id no. 00003','filipapereira306@gmail.com', 'Que maravilho Moet Chandon vinho do Porto!', '10', 'website feedback'),
  ('object id no. 00004','ccj.gmr@gmail.com', 'Desgostoso com o vinho alentejano', '1', 'website feedback'),
  ('object id no. 00005','diegojeffersonms@gmail.com', 'Inacreditável! Um verdedeiro elixir!!!', '10', 'website feedback');

CREATE TABLE website_feedback_processada (
    id                      SERIAL PRIMARY KEY,
    r_id                    INTEGER,
    comentarios             TEXT,
    processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    applied_tas             BOOLEAN DEFAULT FALSE
);

-- VENDAS
CREATE TABLE vendas (
    id                      SERIAL PRIMARY KEY,
    userid                  INTEGER,
    datahora                TIMESTAMP WITHOUT TIME ZONE,
    storeid                 INTEGER,
    productid               INTEGER,
    quantity                INTEGER,
    unit_value              DOUBLE PRECISION,
    payment_method          TEXT,
    comentario              TEXT,
    extracted               BOOLEAN DEFAULT FALSE
);

CREATE TABLE vendas_processada (
    id                      SERIAL PRIMARY KEY,
    r_id                    INTEGER,
    comentario              TEXT,
    processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    applied_tas             BOOLEAN DEFAULT FALSE
);

-- SENTIMENTOS (histórico)
CREATE TABLE sentimentos (
    id                      SERIAL PRIMARY KEY,
    userid                  INTEGER,
    text                    TEXT,       --- corresponde ao texto que foi tratado
    datahora                TIMESTAMP WITHOUT TIME ZONE,
    modelo                  TEXT,       --- corresponde ao modelo com que foi tratado o texto para gerar o sentimento/score
    sentimento              TEXT,
    score                   DOUBLE PRECISION,
    created_on              TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"""

# ----------------------------
# 3) Runner
# ----------------------------
def connect_DATABASE():
    dsn_database = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB_DATABASE} user={PG_USER} password={PG_PASS}"
    print(dsn_database)
    return psycopg2.connect(dsn_database)

def connect():
    dsn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"
    return psycopg2.connect(dsn)

def run_schema():

    with closing(connect_DATABASE()) as connDB, closing(connDB.cursor()) as curDB:
        connDB.autocommit = True  # IMPORTANTE: Ativar autocommit
        curDB.execute(SQL_SCHEMA_DATABASE)
        connDB.commit()

    with closing(connect()) as conn, closing(conn.cursor()) as cur:
        conn.autocommit = True  # IMPORTANTE: Ativar autocommit
        cur.execute(SQL_SCHEMA)
        conn.commit()

    print("Esquema de Database/Postgres (esd_wine) criado/repôsto com sucesso.")
