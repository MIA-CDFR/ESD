# limpa_mongo.py
# -*- coding: utf-8 -*-

from pymongo import MongoClient
from configs import mongodb as mongo

# --- Configurações do MongoDB ---
def limpar_mongo_esd_wine():
    try:
        db_name = mongo.DB_NAME
        
        print(f"A conectar ao MongoDB ...")
        client = MongoClient(mongo.MONGO_URI, serverSelectionTimeoutMS=5000)

        # Selecionar a BD
        db = client[db_name]

        # Apagar a BD MongoDB
        client.drop_database(db_name)

        print(f"Base MongoDB '{db_name}' eliminada com sucesso!")

        # Fechar ligação
        client.close()
        return True

    except Exception as e:
        print(f"Erro ao apagar MongoDB '{db_name}': {e}")
        return False
