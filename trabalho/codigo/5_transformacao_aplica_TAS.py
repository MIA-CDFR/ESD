from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch, numpy as np
from configs import config as config
import database as database
from contextlib import closing
from psycopg2 import sql
import database as database
from contextlib import closing
from psycopg2 import sql
from datetime import datetime

MODEL = config.SENTIMENT_MODEL_USED
tok = AutoTokenizer.from_pretrained(MODEL)
mdl = AutoModelForSequenceClassification.from_pretrained(MODEL)
LABELS = ["NEGATIVO","NEUTRO","POSITIVO"]

def aplica_TAS(texto: str):
    if not texto or not texto.strip():
        return ("NEUTRO", 0.0, MODEL)

    inputs = tok(texto, truncation=True, max_length=256, return_tensors="pt")
    with torch.no_grad():
        out = mdl(**inputs).logits
        probs = torch.softmax(out, dim=1).numpy().flatten()

    idx = int(np.argmax(probs))
    return (LABELS[idx], float(probs[idx]), MODEL)


def processar_varias_tabelas_processada_e_inserir_sentimento(database,
    table,
    table_processada,
    campo_texto,
    batch_size
) -> int:
    """
    Lê registos venda_processada.applied_tas = FALSE,
    aplica análise de sentimento ao 'comentario'
    e insere em sentimento.
    Depois marca venda_processada.applied_tas = TRUE

    Retorna: nº de registos inseridos na tabela sentimento.
    """

    conn = database.connect_database()
    if conn is None:
        print("Falha na conexão à base de dados")
        return 0

    inseridos = 0
    try:
        with closing(conn.cursor()) as cur:
            select_cols = ["id", "r_id", campo_texto]
            
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
            """
            cur.execute(sql.SQL("""
                SELECT {cols}
                  FROM {src}
                 WHERE applied_tas = false
                 ORDER BY id ASC
            """).format(
                cols=sql.SQL(", ").join(map(sql.Identifier, select_cols)),
                src=sql.Identifier(table_processada)
            ))
            rows = cur.fetchall()

        if not rows:
            print(f"Não há registos por processar/aplicar TAS em {table_processada}}.")
            return 0

        with closing(conn.cursor()) as cur:
            buf = 0
            for row in rows:
                rowd = {select_cols[i]: row[i] for i in range(len(select_cols))}

                email = ""
                utilizador_id = 0

                # 1. Obter a data e hora atual (com fuso horário ou sem) 
                #   Exemplo: 2025-10-31 00:05:24.130000
                datahora = datetime.now()
                # 2. Aplicar o método strftime() com o formato desejado
                #   Exemplo: 2025-10-31 00:11:26
                datahora = datahora.strftime("%Y-%m-%d %H:%M:%S")
                
                # --- Usar sql.SQL e .format() para inserir o nome da tabela de forma segura ---
                cur.execute(sql.SQL("""
                    SELECT datahora
                    FROM {table}
                    WHERE id = %s
                """).format(
                    table=sql.Identifier(table) # Injeta o nome da tabela de forma segura
                ), [rowd["r_id"]]) # O %s (placeholder) é substituído pelo r_id
                
                wf_datahora = cur.fetchone()

                if wf_datahora:
                    datahora = wf_datahora[0]

                if (table_processada == "website_feedback_processada"):
                    # Vou buscar o email na tabela website_feedback onde
                    #   website_feedback.id = website_feedback_processada.r_id 
                    cur.execute("""
                        SELECT email
                        FROM website_feedback
                        WHERE id = %s
                    """, [rowd["r_id"]])
                    
                    wf_email = cur.fetchone()

                    if wf_email:
                        email = wf_email[0]

                utilizador_id = database.get_userid_by_email(database, email)

                # Aplicar análise de sentimento
                texto = rowd[campo_texto]
                sentimento, score, modelo = aplica_TAS(texto)

                # Colunas da tabela 'sentimento' (ajuste conforme sua necessidade)
                insert_cols = [
                    "utilizador_id",
                    "text",
                    "datahora",
                    "modelo",
                    "sentimento",
                    "score"
                ]
                insert_vals = [
                    utilizador_id,
                    texto,
                    datahora,
                    modelo,
                    sentimento,
                    score
                ]

                """
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
                """
                
                cur.execute(sql.SQL("""
                    INSERT INTO {dst} ({cols})
                    VALUES ({placeholders})
                """).format(
                    dst=sql.Identifier("sentimento"),
                    cols=sql.SQL(", ").join(map(sql.Identifier, insert_cols)),
                    placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(insert_cols))
                ), insert_vals)

                # Marcar origem como processada
                cur.execute(sql.SQL("""
                    UPDATE {src}
                       SET applied_tas = true
                     WHERE id = %s
                """).format(src=sql.Identifier(table_processada)), [rowd["id"]])

                inseridos += 1
                buf += 1
                if buf >= batch_size:
                    conn.commit()
                    buf = 0

            if buf:
                conn.commit()

        print(f"Inseridos {inseridos} registos em 'sentimento' e marcados como extraídos em '{table_processada}'.")
        return inseridos

    except Exception as e:
        conn.rollback()
        print(f"Erro no processamento de '{table_processada}': {e}")
        return inseridos
    finally:
        database.close_database(conn)

def processar_venda_processada_e_inserir_sentimento(database,
    table: str = "venda",
    table_processada: str = "venda_processada",
    campo_texto: str = "comentario",
    batch_size: int = 100
) -> int:
    """
    Lê registos venda_processada.applied_tas = FALSE,
    aplica análise de sentimento ao 'comentario'
    e insere em sentimento.
    Depois marca venda_processada.applied_tas = TRUE

    Retorna: nº de registos inseridos na tabela sentimento.
    """

    conn = database.connect_database()
    if conn is None:
        print("Falha na conexão à base de dados")
        return 0

    inseridos = 0
    try:
        with closing(conn.cursor()) as cur:

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

            select_cols = ["venda_processada_id", "venda_id", campo_texto]
            
            cur.execute(sql.SQL("""
                SELECT {cols}
                  FROM {src}
                 WHERE applied_tas = false
                 ORDER BY id ASC
            """).format(
                cols=sql.SQL(", ").join(map(sql.Identifier, select_cols)),
                src=sql.Identifier(table_processada)
            ))
            rows = cur.fetchall()

        if not rows:
            print("Não há registos por processar/aplicar TAS em venda_processada.")
            return 0

        with closing(conn.cursor()) as cur:
            buf = 0
            for row in rows:
                rowd = {select_cols[i]: row[i] for i in range(len(select_cols))}
                
                utilizador_id = 0           

                # 1. Obter a data e hora atual (com fuso horário ou sem) 
                #   Exemplo: 2025-10-31 00:05:24.130000
                datahora = datetime.now()
                # 2. Aplicar o método strftime() com o formato desejado
                #   Exemplo: 2025-10-31 00:11:26
                datahora = datahora.strftime("%Y-%m-%d %H:%M:%S")

                # Vou buscar o utilizador_id, datahora na tabela venda onde
                #   venda.venda_id = venda_processada.venda_id 
                cur.execute("""
                    SELECT utilizador_id, datahora
                      FROM venda
                     WHERE venda_id = %s
                """, [rowd["venda_id"]])
                
                vnd_data = cur.fetchone()

                if vnd_data:
                    utilizador_id = vnd_data[0]
                    datahora = vnd_data[1]
                
                # Aplicar análise de sentimento
                texto = rowd[campo_texto]
                sentimento, score, modelo = aplica_TAS(texto)

                # Colunas da tabela 'sentimento' (ajuste conforme sua necessidade)
                insert_cols = [
                    "utilizador_id",
                    "text",
                    "datahora",
                    "modelo",
                    "sentimento",
                    "score"
                ]
                insert_vals = [
                    utilizador_id,
                    texto,
                    datahora,
                    modelo,
                    sentimento,
                    score
                ]

                """
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
                """

                cur.execute(sql.SQL("""
                    INSERT INTO {dst} ({cols})
                    VALUES ({placeholders})
                """).format(
                    dst=sql.Identifier("sentimento"),
                    cols=sql.SQL(", ").join(map(sql.Identifier, insert_cols)),
                    placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(insert_cols))  # Corrigido
                ), insert_vals)

                # Marcar origem como processada
                cur.execute(sql.SQL("""
                    UPDATE {src}
                       SET applied_tas = true
                     WHERE venda_id = %s
                """).format(src=sql.Identifier(table_processada)), [rowd["venda_id"]])

                inseridos += 1
                buf += 1
                if buf >= batch_size:
                    conn.commit()
                    buf = 0

            if buf:
                conn.commit()

        print(f"Inseridos {inseridos} registos em 'sentimento' e marcados como extraídos em '{table_processada}'.")
        return inseridos

    except Exception as e:
        conn.rollback()
        print(f"Erro no processamento de '{table_processada}': {e}")
        return inseridos
    finally:
        database.close_database(conn)


if __name__ == "__main__":

    processar_varias_tabelas_processada_e_inserir_sentimento(
        database,
        table="feeds_rss",
        table_processada="feeds_rss_processada",
        campo_texto="sumario",
        batch_size=100
    )
    
    processar_varias_tabelas_processada_e_inserir_sentimento(
        database,
        table="file_csv_comentarios",
        table_processada="file_csv_comentarios_processada",
        campo_texto="texto",
        batch_size=100
    )
    
    processar_varias_tabelas_processada_e_inserir_sentimento(
        database,
        table="website_feedback",
        table_processada="website_feedback_processada",
        campo_texto="comentarios",
        batch_size=100
    )

    processar_venda_processada_e_inserir_sentimento(
        database,
        table="venda",
        table_processada="venda_processada",
        campo_texto="comentario",
        batch_size=100
    )