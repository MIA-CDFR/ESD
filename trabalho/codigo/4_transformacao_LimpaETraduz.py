# -*- coding: utf-8 -*-
"""
Pipeline de processamento para feeds_rss:
1) Limpar HTML/URLs
2) Lowercase
3) Expandir entidades HTML
4) Normalizar espaços
5) Detectar idioma
6) Traduzir (se != PT)
7) Tokenizar
8) Lematizar
9) Remover stopwords
10) Remover pontuação residual

Depois: inserir em feeds_rss_processada e marcar origem como processed.
"""

import re
import html
import unicodedata
from typing import List, Tuple, Dict, Optional
from contextlib import closing
from psycopg2 import sql
from deep_translator import GoogleTranslator
from langdetect import detect, LangDetectException
import database as database
from typing import List, Dict
from contextlib import closing
from psycopg2 import sql
import datetime

# spaCy / NLTK (com fallback)
try:
    import spacy
    NLP_PT = spacy.load("pt_core_news_sm")
    NLP_EN = spacy.load("en_core_web_sm")
except Exception:
    NLP_PT = NLP_EN = None

try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.stem.snowball import SnowballStemmer
    try:
        _ = stopwords.words("portuguese")
    except LookupError:
        nltk.download("stopwords")
    STOP_PT = set(stopwords.words("portuguese"))
    STOP_EN = set(stopwords.words("english"))
    STEM_PT = SnowballStemmer("portuguese")
    STEM_EN = SnowballStemmer("english")
except Exception:
    STOP_PT = STOP_EN = set()
    STEM_PT = STEM_EN = None

# Idiomas suportados pelo GoogleTranslator
try:
    _gt = GoogleTranslator(source="auto", target="pt")
    SUPPORTED = _gt.get_supported_languages(as_dict=True)
    SUPPORTED_CODES = set(SUPPORTED.values())
except Exception as e:
    print(f"Aviso: não consegui obter idiomas suportados ({e})")
    SUPPORTED = {}
    SUPPORTED_CODES = set()
LANG_ALIASES = {"PT": "pt", "PT-PT": "pt", "PT-BR": "pt", "EN": "en", "EN-GB": "en", "EN-US": "en",
                "ZH": "zh-CN", "ZH-CN": "zh-CN", "ZH-TW": "zh-TW"}


# ===== Passo 1: Limpar HTML / URLs / lixo =====
RE_HTML = re.compile(r"<[^>]+>")
RE_URLS = re.compile(r"https?://\S+|www\.\S+")
def step1_strip_html_urls(s: Optional[str]) -> str:
    if not s:
        return ""
    s = html.unescape(s)  # Passo 3 (expandir entidades HTML) já aqui
    s = RE_HTML.sub(" ", s)
    s = RE_URLS.sub(" ", s)
    return s

# ===== Passo 2: Lowercase =====
def step2_lower(s: str) -> str:
    return s.lower()

# ===== Passo 4: Normalizar espaços =====
SPACES = re.compile(r"\s+")
def step4_normalize_spaces(s: str) -> str:
    return SPACES.sub(" ", s).strip()

# ===== Passo 5: Detetar idioma =====
def step5_detect_lang(s: str) -> str:
    if not s or len(s) < 3:
        return "auto"
    try:
        return detect(s)  # 'pt', 'en', ...
    except LangDetectException:
        return "auto"

def _normalize_lang_code(lang: str) -> str:
    if not lang:
        return "auto"
    if lang.upper() in LANG_ALIASES:
        return LANG_ALIASES[lang.upper()]
    l = lang.lower()
    if l in SUPPORTED_CODES:  # já é código válido
        return l
    if "-" in l:
        base = l.split("-")[0]
        if base in SUPPORTED_CODES:
            return base
    if l in SUPPORTED:  # nome da língua
        return SUPPORTED[l]
    return "auto"

# ===== Passo 6: Traduzir se != PT =====
def step6_translate_if_needed(text: str, src_lang: str, target_lang: str = "pt") -> str:
    if not text:
        return text
    src = _normalize_lang_code(src_lang)
    tgt = _normalize_lang_code(target_lang)
    if src == "pt":
        return text
    try:
        return GoogleTranslator(source=src, target=tgt).translate(text)
    except Exception:
        try:
            return GoogleTranslator(source="auto", target=tgt).translate(text)
        except Exception:
            return text  # fallback

# ===== Passo 7: Tokenizar =====
def step7_tokenize(s: str) -> List[str]:
    return s.split()

# ===== Passo 8: Lematizar (spaCy) / stemmer fallback =====
def step8_lemmatize(tokens: List[str], lang: str = "pt") -> List[str]:
    if not tokens:
        return tokens
    text = " ".join(tokens)
    if lang == "pt" and NLP_PT:
        return [t.lemma_ for t in NLP_PT(text)]
    if lang == "en" and NLP_EN:
        return [t.lemma_ for t in NLP_EN(text)]
    # Fallback: stemmer
    if lang == "pt" and STEM_PT:
        return [STEM_PT.stem(t) for t in tokens]
    if lang == "en" and STEM_EN:
        return [STEM_EN.stem(t) for t in tokens]
    return tokens

# ===== Passo 9: Remover stopwords =====
def step9_remove_stopwords(tokens: List[str], lang: str = "pt") -> List[str]:
    if not tokens:
        return tokens
    if lang == "pt" and STOP_PT:
        return [t for t in tokens if t not in STOP_PT]
    if lang == "en" and STOP_EN:
        return [t for t in tokens if t not in STOP_EN]
    # default pt
    if STOP_PT:
        return [t for t in tokens if t not in STOP_PT]
    return tokens

# ===== Passo 10: Remover pontuação residual =====
RE_PUNCT = re.compile(r"[^\w\s]", re.UNICODE)
def step10_remove_punct(s: str) -> str:
    return RE_PUNCT.sub(" ", s)

# ===== Aux: Remover acentos/cedilhas (se precisares numa etapa posterior) =====
def remove_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    return nfkd.encode("ascii", "ignore").decode("ascii")

# ===== Função que encadeia os 10 passos para um campo =====
def process_text_pipeline(raw_text: Optional[str], force_lang_hint: Optional[str] = None) -> Tuple[str, str]:
    """
    Aplica os 10 passos e retorna:
    - texto_final (str) já processado em PT
    - idioma_detectado (str) antes da tradução
    """
    # 1,2,3,4
    t = step1_strip_html_urls(raw_text)
    t = step2_lower(t)
    t = step4_normalize_spaces(t)

    # 5 (usa hint se fornecido; senão deteta)
    lang = force_lang_hint or step5_detect_lang(t)

    # 6
    t = step6_translate_if_needed(t, lang, "pt")

    # 7
    toks = step7_tokenize(t)

    # 8
    toks = step8_lemmatize(toks, "pt")

    # 9
    toks = step9_remove_stopwords(toks, "pt")

    # 10
    t = " ".join(toks)
    t = step10_remove_punct(t)
    t = step4_normalize_spaces(t)

    return t, lang


# ================== DB JOB ==================

def processar_feeds_rss_e_inserir_processada(database, 
    table_src: str, 
    table_dst: str,
    campos: List[str] = ["titulo", "sumario"],
    batch_size: int = 100
) -> int:
    
    """
    Lê registos feeds_rss.extracted = FALSE,
        processa ["titulo", "sumario"] (10 passos)
        e insere em feeds_rss_processada.
    Depois marca a feeds_rss.extracted = TRUE

    Retorna: nº de registos inseridos na tabela processada.
    """

    conn = database.connect_database()
    if conn is None:
        print("Falha na conexão à base de dados")
        return 0

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
        data_publicacao         TIMESTAMP WITHOUT TIME ZONE,
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
    """

    inseridos = 0
    try:
        with closing(conn.cursor()) as cur:

            select_cols = ["id"] + campos  # Concatena as duas listas

            cur.execute(sql.SQL("SELECT {} FROM {} WHERE processed = false ORDER BY id ASC").format(
                sql.SQL(", ").join(map(sql.Identifier, select_cols)),
                sql.Identifier(table_src)
            ))
            rows = cur.fetchall()

        if not rows:
            print("Não há registos por processar em feeds_rss.")
            return 0

        with closing(conn.cursor()) as cur:
            buf = 0
            for row in rows:
                rowd = {select_cols[i]: row[i] for i in range(len(select_cols))}

                # processar cada campo pedido
                processed_fields: Dict[str, str] = {}
                idioma_detectado_agregado = None

                for campo in campos:
                    texto_proc, idioma_detectado = process_text_pipeline(rowd.get(campo))
                    processed_fields[campo] = texto_proc
                    # guarda o primeiro idioma detetado válido para registo (só informativo)
                    if not idioma_detectado_agregado or idioma_detectado_agregado == "auto":
                        idioma_detectado_agregado = idioma_detectado

                    """
                    -- FEEDS
                    CREATE TABLE feeds_rss_processada (
                        id                      SERIAL PRIMARY KEY,
                        r_id                    INTEGER,
                        titulo                  TEXT,
                        sumario                 TEXT,
                        processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        applied_tas             BOOLEAN DEFAULT FALSE
                    );
                    """

                insert_cols = ["r_id"] + campos
                insert_vals = [rowd["id"]] + [processed_fields[campo] for campo in campos]
                
                cur.execute(sql.SQL("""
                    INSERT INTO {dst} ({cols})
                    VALUES ({placeholders})
                """).format(
                    dst=sql.Identifier(table_dst),
                    cols=sql.SQL(", ").join(map(sql.Identifier, insert_cols)),
                    placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(insert_cols))
                ), insert_vals)

                # marcar origem como processada
                cur.execute(sql.SQL("""
                    UPDATE {src}
                    SET processed = true
                    WHERE id = %s
                """).format(
                    src=sql.Identifier(table_src)
                ), [rowd["id"]])

                inseridos += 1
                buf += 1
                if buf >= batch_size:
                    conn.commit()
                    buf = 0

            if buf:
                conn.commit()

        print(f"Inseridos {inseridos} registos na base dados/Postgres em '{table_dst}' e marcados como processados em '{table_src}'.")
        return inseridos

    except Exception as e:
        conn.rollback()
        print(f"Erro no processamento: {e}")
        return inseridos
    finally:
        database.close_database(conn)

def processar_file_csv_comentarios_e_inserir_processada(database,
    table_src: str = "file_csv_comentarios",
    table_dst: str = "file_csv_comentarios_processada",
    campo_texto: str = "texto",
    batch_size: int = 100
) -> int:
    
    """
    Lê registos file_csv_comentarios.extracted = FALSE,
        processa 'texto' (10 passos)
        e insere em file_csv_comentarios_processada.
    Depois marca a file_csv_comentarios.extracted = true

    Retorna: nº de registos inseridos na tabela processada.
    """

    conn = database.connect_database()
    if conn is None:
        print("Falha na conexão à base de dados")
        return 0
    
    """
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
    """

    inseridos = 0
    try:
        with closing(conn.cursor()) as cur:
            
            select_cols = [
                "id",
                campo_texto
            ]

            cur.execute(sql.SQL("""
                SELECT {cols}
                  FROM {src}
                 WHERE processed = false
                 ORDER BY id ASC
            """).format(
                cols=sql.SQL(", ").join(map(sql.Identifier, select_cols)),
                src=sql.Identifier(table_src)
            ))
            rows = cur.fetchall()

        if not rows:
            print("Não há registos por processar em files_csv_comentarios.")
            return 0

        with closing(conn.cursor()) as cur:
            buf = 0
            for row in rows:
                rowd = {select_cols[i]: row[i] for i in range(len(select_cols))}

                # === aplicar pipeline (10 passos) ao campo 'texto' ===
                texto_proc, idioma_detectado = process_text_pipeline(rowd.get(campo_texto))

                """
                CREATE TABLE file_csv_comentarios_processada (
                    id                      SERIAL PRIMARY KEY,
                    r_id                    INTEGER,
                    texto                   TEXT,
                    processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    applied_tas             BOOLEAN DEFAULT FALSE
                );
                """

                # Preparar INSERT na processada
                insert_cols = [
                    "r_id",
                    "texto"
                ]
                insert_vals = [
                    rowd["id"],
                    texto_proc
                ]

                cur.execute(sql.SQL("""
                    INSERT INTO {dst} ({cols})
                    VALUES ({placeholders})
                """).format(
                    dst=sql.Identifier(table_dst),
                    cols=sql.SQL(", ").join(map(sql.Identifier, insert_cols)),
                    placeholders=sql.SQL(", ").join(sql.Placeholder() * len(insert_cols))
                ), insert_vals)

                # Marcar origem como processada
                cur.execute(sql.SQL("""
                    UPDATE {src}
                       SET processed = true
                     WHERE id = %s
                """).format(src=sql.Identifier(table_src)), [rowd["id"]])

                inseridos += 1
                buf += 1
                if buf >= batch_size:
                    conn.commit()
                    buf = 0

            if buf:
                conn.commit()

        print(f"Inseridos {inseridos} registos na base dados/Postgres em '{table_dst}' e marcados como processados em '{table_src}'.")
        return inseridos

    except Exception as e:
        conn.rollback()
        print(f"Erro no processamento de '{table_src}': {e}")
        return inseridos
    finally:
        database.close_database(conn)

def processar_website_feedback_e_inserir_processada(
    database,
    table_src: str = "website_feedback",
    table_dst: str = "website_feedback_processada",
    campo_texto: str = "comentarios",
    batch_size: int = 100
) -> int:
    
    """
    Lê registos website_feedback.extracted = FALSE,
        processa 'comentarios' (10 passos)
        e insere em website_feedback_processada.
    Depois marca a website_feedback.extracted = true

    Retorna: nº de registos inseridos na tabela processada.
    """

    conn = database.connect_database()
    if conn is None:
        print("Falha na conexão à base de dados")
        return 0
    
    """
    -- WEBSITE FEEDBACK
    CREATE TABLE website_feedback (
        id                      SERIAL PRIMARY KEY,
        object_id               TEXT,
        email                   TEXT,
        comentarios             TEXT,
        classificacao           INTEGER,
        ingested_on             TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        source                  TEXT DEFAULT 'website feedback',
        extracted               BOOLEAN DEFAULT FALSE
    );

    CREATE TABLE website_feedback_processada (
        id                      SERIAL PRIMARY KEY,
        r_id                    INTEGER,
        comentarios             TEXT,
        processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        applied_tas             BOOLEAN DEFAULT FALSE
    );
    """

    inseridos = 0
    try:
        with closing(conn.cursor()) as cur:
            
            select_cols = [
                "id",
                campo_texto
            ]

            cur.execute(sql.SQL("""
                SELECT {cols}
                  FROM {src}
                 WHERE extracted = false
                 ORDER BY id ASC
            """).format(
                cols=sql.SQL(", ").join(map(sql.Identifier, select_cols)),
                src=sql.Identifier(table_src)
            ))
            rows = cur.fetchall()

        if not rows:
            print("Não há registos por extrair/processar em website_feedback.")
            return 0

        with closing(conn.cursor()) as cur:
            buf = 0
            for row in rows:
                rowd = {select_cols[i]: row[i] for i in range(len(select_cols))}

                # Aplicar pipeline (10 passos) ao campo 'comentarios'
                comentarios_proc, idioma_detectado = process_text_pipeline(rowd.get(campo_texto))
                
                """
                CREATE TABLE website_feedback_processada (
                    id                      SERIAL PRIMARY KEY,
                    r_id                    INTEGER,
                    comentarios             TEXT,
                    processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    applied_tas             BOOLEAN DEFAULT FALSE
                );
                """

                insert_cols = [
                    "r_id",
                    "comentarios"
                ]
                insert_vals = [
                    rowd["id"],
                    comentarios_proc
                ]

                cur.execute(sql.SQL("""
                    INSERT INTO {dst} ({cols})
                    VALUES ({placeholders})
                """).format(
                    dst=sql.Identifier(table_dst),
                    cols=sql.SQL(", ").join(map(sql.Identifier, insert_cols)),
                    placeholders=sql.SQL(", ").join(sql.Placeholder() * len(insert_cols))
                ), insert_vals)

                # Marcar origem como extraída
                cur.execute(sql.SQL("""
                    UPDATE {src}
                       SET extracted = true
                     WHERE id = %s
                """).format(src=sql.Identifier(table_src)), [rowd["id"]])

                inseridos += 1
                buf += 1
                if buf >= batch_size:
                    conn.commit()
                    buf = 0

            if buf:
                conn.commit()

        print(f"Inseridos {inseridos} registos em '{table_dst}' e marcados como extraídos em '{table_src}'.")
        return inseridos

    except Exception as e:
        conn.rollback()
        print(f"Erro no processamento de '{table_src}': {e}")
        return inseridos
    finally:
        database.close_database(conn)

def processar_vendas_e_inserir_processada(
    database,
    table_src: str = "vendas",
    table_dst: str = "vendas_processada",
    campo_texto: str = "comentario",
    batch_size: int = 100
) -> int:
    
    """
    Lê registos vendas.extracted = FALSE,
        processa 'comentario' (10 passos)
        e insere em vendas_processada.
    Depois marca a vendas.extracted = true

    Retorna: nº de registos inseridos na tabela processada.
    """

    conn = database.connect_database()
    if conn is None:
        print("Falha na conexão à base de dados")
        return 0

    """	
    -- VENDAS
    CREATE TABLE vendas (
        id                      SERIAL PRIMARY KEY,
        userid                  INTEGER,
        datahora                TIMESTAMP WITHOUT TIME ZONE NOT NULL,
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
        comentario_processado   TEXT,
        processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        applied_tas             BOOLEAN DEFAULT FALSE
    );
    """

    inseridos = 0
    try:
        with closing(conn.cursor()) as cur:
            
            select_cols = [
                "id",
                campo_texto
            ]

            cur.execute(sql.SQL("""
                SELECT {cols}
                  FROM {src}
                 WHERE extracted = false
                 ORDER BY id ASC
            """).format(
                cols=sql.SQL(", ").join(map(sql.Identifier, select_cols)),
                src=sql.Identifier(table_src)
            ))
            rows = cur.fetchall()

        if not rows:
            print("Não há registos por extrair/processar em vendas.")
            return 0

        with closing(conn.cursor()) as cur:
            buf = 0
            for row in rows:
                rowd = {select_cols[i]: row[i] for i in range(len(select_cols))}

                # Aplicar pipeline (10 passos) ao campo 'comentarios'
                comentario_proc, idioma_detectado = process_text_pipeline(rowd.get(campo_texto))

                """	
                -- VENDAS

                CREATE TABLE vendas_processada (
                    id                      SERIAL PRIMARY KEY,
                    r_id                    INTEGER,
                    comentario              TEXT,
                    processed_on            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    applied_tas             BOOLEAN DEFAULT FALSE
                );
                """
                
                insert_cols = [
                    "r_id",
                    campo_texto
                ]
                insert_vals = [
                    rowd["id"],
                    comentario_proc
                ]

                cur.execute(sql.SQL("""
                    INSERT INTO {dst} ({cols})
                    VALUES ({placeholders})
                """).format(
                    dst=sql.Identifier(table_dst),
                    cols=sql.SQL(", ").join(map(sql.Identifier, insert_cols)),
                    placeholders=sql.SQL(", ").join(sql.Placeholder() * len(insert_cols))
                ), insert_vals)

                # Marcar origem como extraída
                cur.execute(sql.SQL("""
                    UPDATE {src}
                       SET extracted = true
                     WHERE id = %s
                """).format(src=sql.Identifier(table_src)), [rowd["id"]])

                inseridos += 1
                buf += 1
                if buf >= batch_size:
                    conn.commit()
                    buf = 0

            if buf:
                conn.commit()

        print(f"Inseridos {inseridos} registos em '{table_dst}' e marcados como extraídos em '{table_src}'.")
        return inseridos

    except Exception as e:
        conn.rollback()
        print(f"Erro no processamento de '{table_src}': {e}")
        return inseridos
    finally:
        database.close_database(conn)


if __name__ == "__main__":

    processar_feeds_rss_e_inserir_processada(
        database,
        table_src="feeds_rss",
        table_dst="feeds_rss_processada",
        campos=["titulo", "sumario"],
        batch_size=100
    )

    processar_file_csv_comentarios_e_inserir_processada(
        database,
        table_src="file_csv_comentarios",
        table_dst="file_csv_comentarios_processada",
        campo_texto="texto",
        batch_size=100
    )

    processar_website_feedback_e_inserir_processada(
        database,
        table_src="website_feedback",
        table_dst="website_feedback_processada",
        campo_texto="comentarios",
        batch_size=100
    )

    processar_vendas_e_inserir_processada(
        database,
        table_src="vendas",
        table_dst="vendas_processada",
        campo_texto="comentario",
        batch_size=100
    )