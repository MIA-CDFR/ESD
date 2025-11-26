
# python.exe -m pip install sqlalchemy psycopg2-binary
"""
segmentacao_kmeans.py

Fluxo:
1. (Re)cria a tabela feature_utilizador a partir das tabelas transacionais
2. Lê a tabela feature_utilizador
3. Aplica K-Means para segmentação de clientes
4. Grava os clusters na tabela public.utilizador_segmento
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans


# ============================================================
# CONFIGURAÇÃO DA BASE DE DADOS
# ============================================================

DATABASE_URL = "postgresql://local:local@localhost:4000/esd_wine"  # <-- ajusta aqui

# ============================================================
# SQL PARA CRIAR / LER FEATURES E TABELA DE SEGMENTOS
# ============================================================

SQL_CREATE_FEATURE_TABLE = """
DROP TABLE IF EXISTS public.feature_utilizador;

CREATE TABLE public.feature_utilizador AS
WITH vendas_agg AS (
    SELECT
        v.utilizador_id,
        MIN(v.datahora::date)                               AS primeira_compra,
        MAX(v.datahora::date)                               AS ultima_compra,
        COUNT(*)                                            AS n_compras,
        SUM(v.quantidade)                                   AS quantidade_total,
        SUM(v.quantidade * v.valor_unitario)               AS valor_total,
        COUNT(DISTINCT v.produto_id)                        AS n_produtos,
        COUNT(DISTINCT v.loja_id)                           AS n_lojas,
        COUNT(DISTINCT p.regiao)                            AS n_regioes
    FROM public.venda v
    JOIN public.produto p
        ON v.produto_id = p.produto_id
    GROUP BY v.utilizador_id
),
sentimento_agg AS (
    SELECT
        s.utilizador_id,
        AVG(s.score)       AS score_sentimento_medio,
        COUNT(*)           AS n_registos_sentimento
    FROM public.sentimento s
    GROUP BY s.utilizador_id
),
comentarios_agg AS (
    SELECT
        c.utilizador_id,
        COUNT(*) AS n_comentarios
    FROM public.file_csv_comentarios c
    GROUP BY c.utilizador_id
)
SELECT
    u.utilizador_id,

    -- idade em anos
    date_part('year', age(current_date, u.data_nascimento))::integer AS idade,

    v.n_compras,
    v.quantidade_total,
    v.valor_total,

    CASE 
        WHEN v.n_compras > 0 THEN (v.valor_total / v.n_compras) 
        ELSE 0 
    END AS ticket_medio,

    --EXTRACT(DAY FROM (current_date - v.ultima_compra))::integer AS recencia_dias,
    (current_date - v.ultima_compra)::integer AS recencia_dias,
    v.n_produtos,
    v.n_lojas,
    v.n_regioes,

    COALESCE(s.score_sentimento_medio, 0)  AS score_sentimento_medio,
    COALESCE(s.n_registos_sentimento, 0)   AS n_registos_sentimento,
    COALESCE(c.n_comentarios, 0)           AS n_comentarios

FROM public.utilizador u
JOIN vendas_agg v
    ON u.utilizador_id = v.utilizador_id
LEFT JOIN sentimento_agg s
    ON u.utilizador_id = s.utilizador_id
LEFT JOIN comentarios_agg c
    ON u.utilizador_id = c.utilizador_id;
"""

SQL_SELECT_FEATURES = """
SELECT
    utilizador_id,
    idade,
    n_compras,
    quantidade_total,
    valor_total,
    ticket_medio,
    recencia_dias,
    n_produtos,
    n_lojas,
    n_regioes,
    score_sentimento_medio,
    n_registos_sentimento,
    n_comentarios
FROM public.feature_utilizador;
"""

SQL_CREATE_SEGMENT_TABLE = """
CREATE TABLE IF NOT EXISTS public.utilizador_segmento (
    utilizador_id integer PRIMARY KEY,
    cluster integer NOT NULL,
    atualizado_em timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);
"""


# ============================================================
# FUNÇÕES
# ============================================================

def get_engine():
    return create_engine(DATABASE_URL)


def rebuild_feature_table(engine):
    """Recria a tabela de features."""
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATE_FEATURE_TABLE))


def load_features(engine) -> pd.DataFrame:
    df = pd.read_sql(SQL_SELECT_FEATURES, engine)
    if df.empty:
        raise ValueError("Tabela feature_utilizador está vazia. Verifica se há dados em venda/produto/utilizador.")
    return df


def run_kmeans(df: pd.DataFrame, n_clusters: int = 4) -> pd.DataFrame:
    """Aplica K-Means às features e devolve df com utilizador_id + cluster."""
    ids = df['utilizador_id']
    X = df.drop(columns=['utilizador_id'])

    # Substituir valores em falta
    X = X.fillna(0)

    # Escalonar
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Modelo K-Means
    model = KMeans(n_clusters=n_clusters, random_state=42)
    clusters = model.fit_predict(X_scaled)

    return pd.DataFrame({
        'utilizador_id': ids,
        'cluster': clusters
    })


def save_clusters(engine, df_clusters: pd.DataFrame):
    """Grava os clusters na tabela utilizador_segmento."""
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATE_SEGMENT_TABLE))
        conn.execute(text("TRUNCATE TABLE public.utilizador_segmento;"))

    df_clusters.to_sql(
        'utilizador_segmento',
        engine,
        schema='public',
        if_exists='append',
        index=False
    )


def main():
    engine = get_engine()

    # 1. (Re)criar features
    rebuild_feature_table(engine)

    # 2. Ler features
    df_features = load_features(engine)

    # 3. K-Means
    df_clusters = run_kmeans(df_features, n_clusters=4)

    # 4. Gravar clusters
    save_clusters(engine, df_clusters)

    print("Segmentação concluída. Clusters gravados em public.utilizador_segmento.")


if __name__ == "__main__":
    main()
