from scripts import files as file
import pandas as pd
from configs import config as config
import database as database
import pandas as pd
from sqlalchemy import create_engine

def exportar_sentimentos_TOCSV():
    uri = database.get_connection_uri()
    
    try:
        engine = create_engine(uri)
        query = "SELECT id, userid, datahora, text, modelo, sentimento, score, created_on FROM sentimentos;"
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

    exportar_sentimentos_TOCSV()

