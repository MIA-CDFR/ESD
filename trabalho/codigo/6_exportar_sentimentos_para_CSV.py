from scripts import files as file
import pandas as pd
from configs import config as config
import database as database
import pandas as pd
from sqlalchemy import create_engine

def exportar_sentimento_TOCSV():
    uri = database.get_connection_uri()

    """"
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

    try:
        engine = create_engine(uri)
        query = "SELECT sentimento_id, utilizador_id, text, datahora, modelo, sentimento, score, created_on FROM sentimento;"
        df = pd.read_sql_query(query, engine)
        
        if df.empty:
            print("Não há registos...")
            return 0

        # Guardo CSV
        file.save_csv(config.FILENAME_SENTIMENTOS, df)
        return len(df)

    except Exception as e:
        print(f"Erro na exportação: {e}")
        return 0

if __name__ == "__main__":

    exportar_sentimento_TOCSV()

