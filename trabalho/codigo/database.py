import psycopg2
from configs import config as config
import database  # ajusta o import conforme estrutura do teu projeto
from contextlib import closing
from psycopg2 import sql

# ===============================================
# A. CONFIGURAÇÕES DE CONEXÃO
# ===============================================
DB_CONFIG_DATABASE = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}
DB_CONFIG = {
    "dbname": "esd_wine",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

# ===============================================
# B. FUNÇÕES DE GESTÃO DE CONEXÃO
# ===============================================

# Em database.py
def get_connection_uri():
    """Retorna a string URI de conexão no formato que o SQLAlchemy/Pandas prefere."""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

def connect_database():
    """Tenta estabelecer e retorna uma conexão à base de dados."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    
    except Exception as e:
        print(f"ERRO: Não foi possível conectar à base de dados. {e}")
        return None

def close_database(conn):
    """Fecha a conexão à base de dados se ela estiver aberta."""
    if conn is not None:
        try:
            conn.close()
        except Exception as e:
            print(f"Aviso: Erro ao fechar a conexão. {e}")

# ===============================================
# C. FUNÇÕES DE DADOS
# ===============================================

def save_toDatabase(df, table_name):
    print(f"Cheguei a database.py, para save_toDatabase")      # APAGAR

    if table_name == config.DATABASE_TABLENAME_FEEDS_RSS:
        save_toDatabase_FeedsRSS(df, table_name)
    elif table_name == config.DATABASE_TABLENAME_FILE_CSV:
        save_toDatabase_FilesCSV(df, table_name)
    elif table_name == config.DATABASE_TABLENAME_WEBSITE_FEEDBACK:
        save_toDatabase_WebsiteFeedback(df, table_name)
    elif table_name == config.DATABASE_TABLENAME_VENDAS:
        save_toDatabase_Vendas(df, table_name)
    else:
        print("não vou fazer nada")

def save_toDatabase_FeedsRSS(df, table_name, batch_size=config.DATABASE_BATCH_SIZE_FOR_INSERT):

    conn = connect_database()
    if conn is None:
        return # Sair se a conexão falhar

    try:
        cursor = conn.cursor()

        total_inseridos = 0
        
        # Colunas que vamos inserir (SEM recordid e created_at - são automáticos)
        colunas = "fonte_nome, fonte_url, idioma, titulo, link, sumario, datahora"

        """
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
        """

        # Processa em batches para evitar limite de parâmetros do PostgreSQL
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            
            valores_list = []
            params = []
            
            for index, row in batch.iterrows():
                # 7 placeholders para as 7 colunas
                valores_list.append("(%s, %s, %s, %s, %s, %s, %s)")
                params.extend([
                    row['fonte_nome'],
                    row['fonte_url'],
                    row['idioma'],
                    row['titulo'],
                    row['link'],
                    row['sumario'],
                    row['datahora']
                ])
            
            valores_str = ', '.join(valores_list)
            
            query = f"""
                INSERT INTO {table_name} 
                ({colunas}) 
                VALUES {valores_str}
            """
            
            cursor.execute(query, params)
            total_inseridos += len(batch)
        
        conn.commit()
        cursor.close()
        print(f"Inseridos {total_inseridos} comentários em '{table_name}' na base dados/Postgres.")
    except Exception as e:
        print(f"Erro ao inserir registo: {e}")
        # Opcional: Adicionar conn.rollback() aqui se quiser desfazer a transação em caso de erro
    finally:
        # Reutilizamos a função para fechar a conexão
        close_database(conn)

def save_toDatabase_FilesCSV(df, table_name, batch_size=config.DATABASE_BATCH_SIZE_FOR_INSERT):

    conn = connect_database()
    if conn is None:
        return # Sair se a conexão falhar

    try:
        cursor = conn.cursor()

        total_inseridos = 0
        
        # Colunas que vamos inserir (SEM recordid e created_at - são automáticos)
        colunas = "commentid, utilizador_id, produto_id, loja_id, datahora, texto"

        """
        -- COMENTÁRIOS (CSV)
        CREATE TABLE file_csv_comentarios (
            id                      SERIAL PRIMARY KEY,
            commentid               INTEGER,
            utilizador_id           INTEGER,
            produto_id              INTEGER,
            loja_id                 INTEGER,
            datahora                TIMESTAMP WITHOUT TIME ZONE,
            texto                   TEXT,
            extracted_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            processed               BOOLEAN DEFAULT FALSE
        );
        """

        # Processa em batches para evitar limite de parâmetros do PostgreSQL
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            
            valores_list = []
            params = []
            
            for index, row in batch.iterrows():
                # 6 placeholders para as 6 colunas
                valores_list.append("(%s, %s, %s, %s, %s, %s)")
                params.extend([
                    row['commentid'],
                    row['utilizador_id'],
                    row['produto_id'],
                    row['loja_id'],
                    row['datahora'],
                    row['texto']
                ])
            
            valores_str = ', '.join(valores_list)
            
            query = f"""
                INSERT INTO {table_name} 
                ({colunas}) 
                VALUES {valores_str}
            """
            
            cursor.execute(query, params)
            total_inseridos += len(batch)
        
        conn.commit()
        cursor.close()
        print(f"Inseridos {total_inseridos} comentários em '{table_name}' na base dados/Postgres.")
    except Exception as e:
        print(f"Erro ao inserir registo: {e}")
        # Opcional: Adicionar conn.rollback() aqui se quiser desfazer a transação em caso de erro
    finally:
        # Reutilizamos a função para fechar a conexão
        close_database(conn)

def save_toDatabase_WebsiteFeedback(df, table_name, batch_size=config.DATABASE_BATCH_SIZE_FOR_INSERT):

    conn = connect_database()
    if conn is None:
        return # Sair se a conexão falhar

    try:
        cursor = conn.cursor()

        total_inseridos = 0
        
        # Colunas que vamos inserir (SEM recordid e created_at - são automáticos)
        colunas = "object_id, email, comentarios, classificacao"

        """
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
        """

        # Processa em batches para evitar limite de parâmetros do PostgreSQL
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            
            valores_list = []
            params = []
            
            for index, row in batch.iterrows():
                # 4 placeholders para as 4 colunas
                valores_list.append("(%s, %s, %s, %s)")
                params.extend([
                    row['_id'],
                    row['email'],
                    row['comentarios'],
                    row['classificacao']
                ])
            
            valores_str = ', '.join(valores_list)
            
            query = f"""
                INSERT INTO {table_name} 
                ({colunas}) 
                VALUES {valores_str}
            """
            
            cursor.execute(query, params)
            total_inseridos += len(batch)
        
        conn.commit()
        cursor.close()
        print(f"Inseridos {total_inseridos} comentários em '{table_name}' na base dados/Postgres.")
    except Exception as e:
        print(f"Erro ao inserir registo: {e}")
        # Opcional: Adicionar conn.rollback() aqui se quiser desfazer a transação em caso de erro
    finally:
        # Reutilizamos a função para fechar a conexão
        close_database(conn)

def save_toDatabase_Vendas(df, table_name, batch_size=config.DATABASE_BATCH_SIZE_FOR_INSERT):

    conn = connect_database()
    if conn is None:
        return # Sair se a conexão falhar

    try:
        cursor = conn.cursor()

        total_inseridos = 0
    
        # Colunas que vamos inserir (SEM recordid e created_at - são automáticos)
        colunas = "utilizador_id, datahora, loja_id, produto_id, quantidade, valor_unitario, metodo_pagamento_id, comentario"

        """
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
        """

        # Processa em batches para evitar limite de parâmetros do PostgreSQL
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            
            valores_list = []
            params = []
            
            for index, row in batch.iterrows():
                print(f"row['metodo_pagamento_id']: {row['metodo_pagamento_id']}")      # APAGAR
                # 8 placeholders para as 8 colunas
                valores_list.append("(%s, %s, %s, %s, %s, %s, %s, %s)")
                params.extend([
                    row['utilizador_id'],
                    row['datahora'],
                    row['loja_id'],
                    row['produto_id'],
                    row['quantidade'],
                    row['valor_unitario'],
                    row['metodo_pagamento_id'],
                    row['comentario']
                ])
            
            valores_str = ', '.join(valores_list)
            
            query = f"""
                INSERT INTO {table_name} 
                ({colunas}) 
                VALUES {valores_str}
            """
            
            cursor.execute(query, params)
            total_inseridos += len(batch)
        
        conn.commit()
        cursor.close()
        print(f"Inseridos {total_inseridos} comentários em '{table_name}' na base dados/Postgres.")
    except Exception as e:
        print(f"Erro ao inserir registo: {e}")
        # Opcional: Adicionar conn.rollback() aqui se quiser desfazer a transação em caso de erro
    finally:
        # Reutilizamos a função para fechar a conexão
        close_database(conn)

def get_userid_by_email(database, email: str) -> int:
    """
    Procura o utilizador pelo email na tabela 'utilizador'.
    Se existir, retorna o utilizador_id (int).
    Se não existir, retorna 0.
    """
    if not email:
        return 0

    conn = database.connect_database()
    if conn is None:
        print("Falha na conexão à base de dados")
        return 0

    try:
        with closing(conn.cursor()) as cur:
            cur.execute(sql.SQL("""
                SELECT utilizador_id
                  FROM utilizador
                 WHERE email = %s
                 LIMIT 1
            """), (email,))
            row = cur.fetchone()
            return row[0] if row else 0

    except Exception as e:
        print(f"[get_userid_by_email] Erro: {e}")
        return 0
    finally:
        database.close_database(conn)
