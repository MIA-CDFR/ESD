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
-- Drop em ordem para evitar FKs/dep. (é ambiente de dev/demo)
DROP TABLE IF EXISTS feeds_rss                          CASCADE;
DROP TABLE IF EXISTS feeds_rss_processada               CASCADE;
DROP TABLE IF EXISTS file_csv_comentario                CASCADE;
DROP TABLE IF EXISTS file_csv_comentario_processada     CASCADE;
DROP TABLE IF EXISTS website_feedback                   CASCADE;
DROP TABLE IF EXISTS website_feedback_processada        CASCADE;
DROP TABLE IF EXISTS loja                               CASCADE;
DROP TABLE IF EXISTS metodo_pagamento                   CASCADE;
DROP TABLE IF EXISTS produto                            CASCADE;
DROP TABLE IF EXISTS sentimento                         CASCADE;
DROP TABLE IF EXISTS utilizador                         CASCADE;
DROP TABLE IF EXISTS utilizador_email                   CASCADE;
DROP TABLE IF EXISTS venda                              CASCADE;
DROP TABLE IF EXISTS venda_processada                   CASCADE;

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
    utilizador_id           INTEGER,
    produto_id              INTEGER,
    loja_id                 INTEGER,
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

-- LOJA
CREATE TABLE loja (
    loja_id          SERIAL PRIMARY KEY,
    loja             TEXT
);

INSERT INTO loja (loja_id, loja) VALUES
 (1,'Loja on-line'),
 (2,'Loja Braga');

-- METODO_PAGAMENTO
CREATE TABLE metodo_pagamento (
    metodo_pagamento_id     SERIAL PRIMARY KEY,
    metodo_pagamento        TEXT
);

INSERT INTO metodo_pagamento (metodo_pagamento_id, metodo_pagamento) VALUES
  (1,'MULTIBANCO'),
  (2,'TANSFERÊNCIA BANCÁRIA'),
  (3,'CARTÃO DÉBITO'),
  (4,'CARTÃO CRÉDITO');

-- PRODUTO
CREATE TABLE produto (
    produto_id              SERIAL PRIMARY KEY,
    produto                 TEXT,
    regiao                  TEXT,
    safra                   TEXT
);

INSERT INTO produto (produto_id, produto, regiao, safra) VALUES
(1,'Barca Velha','Douro','2011'),
(2,'Quinta do Crasto Vinha Maria Teresa','Douro','2018'),
(3,'Quinta do Vale Meão','Douro','2019'),
(4,'Niepoort Redoma Tinto','Douro','2020'),
(5,'Quinta do Vallado Reserva','Douro','2017'),
(6,'Pêra-Manca Tinto','Alentejo','2015'),
(7,'Herdade do Esporão Reserva','Alentejo','2020'),
(8,'Cartuxa Scala Coeli','Alentejo','2016'),
(9,'Mouchão Tinto','Alentejo','2013'),
(10,'Herdade Grande Gerações','Alentejo','2018'),
(11,'Soalheiro Alvarinho Primeiras Vinhas','Vinho Verde','2022'),
(12,'Anselmo Mendes Parcela Única','Vinho Verde','2021'),
(13,'Quinta de Soalheiro Granit','Vinho Verde','2022'),
(14,'Quinta da Aveleda Loureiro','Vinho Verde','2020'),
(15,'Quinta do Ameal Loureiro','Vinho Verde','2021'),
(16,'Quinta da Leda','Douro','2019'),
(17,'Vinha do Mouro','Alentejo','2018'),
(18,'Quinta do Pessegueiro','Douro','2020'),
(19,'Incógnito Cortes de Cima','Alentejo','2017'),
(20,'Titan of Douro','Douro','2019'),
(21,'Château Margaux','Bordeaux','2010'),
(22,'Château Lafite Rothschild','Bordeaux','2014'),
(23,'Château Latour','Bordeaux','2009'),
(24,'Château Haut-Brion','Bordeaux','2017'),
(25,'Château Mouton Rothschild','Bordeaux','2016'),
(26,'Château Cheval Blanc','Bordeaux','2012'),
(27,'Château Pétrus','Bordeaux','2015'),
(28,'Château Angélus','Bordeaux','2014'),
(29,'Château Pavie','Bordeaux','2016'),
(30,'Château Léoville Las Cases','Bordeaux','2018'),
(31,'Vega Sicilia Único','Ribera del Duero','2009'),
(32,'Pingus','Ribera del Duero','2018'),
(33,'Aalto PS','Ribera del Duero','2019'),
(34,'Emilio Moro Malleolus','Ribera del Duero','2020'),
(35,'Pesquera Reserva','Ribera del Duero','2016'),
(36,'La Rioja Alta 904 Gran Reserva','Rioja','2011'),
(37,'Marqués de Riscal Reserva','Rioja','2018'),
(38,'Muga Prado Enea','Rioja','2015'),
(39,'Contador','Rioja','2017'),
(40,'Remírez de Ganuza Reserva','Rioja','2016'),
(41,'Opus One','Napa Valley','2018'),
(42,'Screaming Eagle Cabernet Sauvignon','Napa Valley','2015'),
(43,'Harlan Estate','Napa Valley','2014'),
(44,'Dominus Estate','Napa Valley','2019'),
(45,'Caymus Special Selection','Napa Valley','2020'),
(46,'Penfolds Grange','Barossa Valley','2017'),
(47,'Torbreck RunRig','Barossa Valley','2016'),
(48,'Henschke Hill of Grace','Eden Valley','2015'),
(49,'Yalumba The Signature','Barossa Valley','2018'),
(50,'Two Hands Bellas Garden','Barossa Valley','2019'),
(51,'Sassicaia','Toscana','2018'),
(52,'Tignanello','Toscana','2020'),
(53,'Ornellaia','Toscana','2017'),
(54,'Masseto','Toscana','2016'),
(55,'Brunello di Montalcino Biondi-Santi','Toscana','2015'),
(56,'Amarone della Valpolicella Dal Forno','Veneto','2012'),
(57,'Quintarelli Amarone','Veneto','2011'),
(58,'Allegrini Amarone','Veneto','2016'),
(59,'Zenato Amarone Riserva','Veneto','2015'),
(60,'Tommasi Amarone','Veneto','2017'),
(61,'Gaja Barbaresco','Piemonte','2019'),
(62,'Gaja Sperss','Piemonte','2017'),
(63,'Bruno Giacosa Barolo Falletto','Piemonte','2016'),
(64,'Paolo Scavino Barolo','Piemonte','2018'),
(65,'Vietti Barolo Ravera','Piemonte','2019'),
(66,'Cloudy Bay Sauvignon Blanc','Marlborough','2023'),
(67,'Dog Point Sauvignon Blanc','Marlborough','2022'),
(68,'Greywacke Sauvignon Blanc','Marlborough','2022'),
(69,'Te Koko Cloudy Bay','Marlborough','2019'),
(70,'Villa Maria Reserve','Marlborough','2021'),
(71,'Tokaji Essencia','Tokaj','2013'),
(72,'Tokaji Aszú 6 Puttonyos','Tokaj','2011'),
(73,'Oremus Tokaji Aszú','Tokaj','2014'),
(74,'Disznókő Aszú 5 Puttonyos','Tokaj','2016'),
(75,'Royal Tokaji Aszú Gold Label','Tokaj','2017'),
(76,'Châteauneuf-du-Pape Château de Beaucastel','Rhône','2018'),
(77,'E.Guigal La Mouline','Rhône','2016'),
(78,'E.Guigal La Landonne','Rhône','2015'),
(79,'E.Guigal La Turque','Rhône','2017'),
(80,'Domaine du Vieux Télégraphe','Rhône','2019'),
(81,'Clos Apalta','Colchagua','2017'),
(82,'Almaviva','Maipo Valley','2018'),
(83,'Seña','Aconcagua','2019'),
(84,'Don Melchor Cabernet Sauvignon','Maipo Valley','2020'),
(85,'Montes Alpha M','Colchagua','2016'),
(86,'Catena Zapata Adrianna Vineyard','Mendoza','2018'),
(87,'Catena Zapata Nicolás Catena','Mendoza','2017'),
(88,'Achaval Ferrer Finca Bella Vista','Mendoza','2019'),
(89,'Zuccardi Finca Piedra Infinita','Mendoza','2018'),
(90,'Rutini Apartado Gran Malbec','Mendoza','2020'),
(91,'Grahams Vintage Port','Douro','2016'),
(92,'Taylors Vintage Port','Douro','2017'),
(93,'Fonseca Vintage Port','Douro','2018'),
(94,'Dows Vintage Port','Douro','2016'),
(95,'Quinta do Noval Nacional','Douro','2011'),
(96,'Krug Grande Cuvée','Champagne','NV'),
(97,'Dom Pérignon','Champagne','2013'),
(98,'Louis Roederer Cristal','Champagne','2014'),
(99,'Bollinger La Grande Année','Champagne','2012'),
(100,'Pol Roger Sir Winston Churchill','Champagne','2013');

-- UTILIZADOR
CREATE TABLE utilizador (
    utilizador_id           SERIAL PRIMARY KEY,
    nome                    TEXT,
    email                   TEXT,
    data_nascimento         TIMESTAMP WITHOUT TIME ZONE,
    genero                  TEXT,
    regiao                  TEXT
);

INSERT INTO utilizador (nome, email, data_nascimento, genero, regiao) VALUES
  ('Desconhecido','','01-01-1900','D','D'),
  ('Carlos','ccj.gmr@gmail.com','13-10-1977','M','Trás Montes'),
  ('Rui','rmmmrodrigues@gmail.com','01-05-1977','M','Minho'),
  ('Filipa','filipapereira306@gmail.com','10-06-2000','F','Minho'),
  ('Diego','diegojeffersonms@gmail.com','15-12-1990','M','Brasil');

-- UTILIZADOR_EMAIL
CREATE TABLE utilizador_email (
    utilizador_id           INTEGER,
    email                   TEXT,
    created_on              TIMESTAMP WITHOUT TIME ZONE
);

INSERT INTO utilizador_email (utilizador_id, email) VALUES
  (1,'carlos@gmail.com'),
  (2,'rmmmrodrigues@gmail.com'),
  (3,'filipapereira306@gmail.com'),
  (4,'diegojeffersonms@gmail.com'),
  (1,'ccj.gmr@gmail.com');

-- SENTIMENTO
CREATE TABLE sentimento (
    sentimento_id           SERIAL PRIMARY KEY,
    utilizador_id           INTEGER,
    text                    TEXT,       --- corresponde ao texto que foi tratado
    datahora                TIMESTAMP WITHOUT TIME ZONE,
    modelo                  TEXT,       --- corresponde ao modelo com que foi tratado o texto para gerar o sentimento/score
    sentimento              TEXT,
    score                   DOUBLE PRECISION,
    created_on              TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT utilizador_fkey FOREIGN KEY (utilizador_id)
        REFERENCES utilizador (utilizador_id) MATCH SIMPLE
);

-- VENDA
CREATE TABLE venda (
    venda_id                SERIAL PRIMARY KEY,
    utilizador_id           INTEGER,
    datahora                TIMESTAMP WITHOUT TIME ZONE,
    loja_id                 INTEGER,
    produto_id              INTEGER,
    quantidade              INTEGER,
    valor_unitario          DOUBLE PRECISION,
    metodo_pagamento_id     INTEGER,
    comentario              TEXT,
    extracted               BOOLEAN DEFAULT FALSE,        
    CONSTRAINT utilizador_fkey FOREIGN KEY (utilizador_id)
        REFERENCES utilizador (utilizador_id) MATCH SIMPLE,
    CONSTRAINT loja_fkey FOREIGN KEY (loja_id)
        REFERENCES loja (loja_id) MATCH SIMPLE,
    CONSTRAINT produto_fkey FOREIGN KEY (produto_id)
        REFERENCES produto (produto_id) MATCH SIMPLE,
    CONSTRAINT metodo_pagamento_fkey FOREIGN KEY (metodo_pagamento_id)
        REFERENCES metodo_pagamento (metodo_pagamento_id) MATCH SIMPLE
);

-- VENDA_PROCESSADA
CREATE TABLE venda_processada (
    venda_processada_id     SERIAL PRIMARY KEY,
    venda_id                INTEGER,
    comentario              TEXT,
    processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    applied_tas             BOOLEAN DEFAULT FALSE,
    CONSTRAINT venda_fkey FOREIGN KEY (venda_id)
        REFERENCES venda (venda_id) MATCH SIMPLE
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
