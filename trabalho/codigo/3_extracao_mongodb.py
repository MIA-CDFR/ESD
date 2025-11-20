import pymongo
from pymongo import MongoClient
import pandas as pd
import json
from configs import config as config, mongodb as mongo
from scripts import files as file
import database as database
from datetime import datetime

# --- Configurações do Ficheiro ---
JSON_FILE = config.FILENAME_SOURCES_TO_IMPORT           # O nome do seu ficheiro JSON, e.g., "sources.json"
DIR_SOURCES = config.DIR_SOURCES                        # O diretório base para procurar os CSVs (deve ser um objeto Path)

# --- Configurações do MongoDB ---
MONGO_URI = mongo.MONGO_URI
DB_NAME = mongo.DB_NAME

def extract_from_mongodb_toDatabase(source):
    # Decarrego todos referentes aos que temos no JSON
    extract_JSON_reference_from_mongodb_toDatabase(source)
    # Decarrego os do website
    extract_WEBSITE_reference_from_mongodb_toDatabase()
    # Decarrego os do website
    extract_vendas_from_mongodb_toDatabase()

def extract_vendas_from_mongodb_toDatabase():
    """
    Conecta-se ao MongoDB e carrega o conteúdo da coleção para um Pandas DataFrame.
    """
    
    try:
        # Conexão ao MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client[DB_NAME]
        print("Conexão com o MongoDB bem-sucedida!")

        # Extracção das colections referentes aos comentarios/website
        
        collection = db["vendas"]   # nome cf source/JSON

        # Encontra todos os documentos na coleção
        cursor = collection.find({})
    
        # Converte o cursor do MongoDB diretamente para uma lista de dicionários
        list_of_documents = list(cursor)
    
        if not list_of_documents:
            print(f"A coleção '{collection}' está vazia ou não existe.")
            return pd.DataFrame()

        # Converte ObjectId para string ANTES de criar DataFrame
        for doc in list_of_documents:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        # Converte a lista de dicionários para um DataFrame do Pandas
        df = pd.DataFrame(list_of_documents)
        print(f"Carregados do MongoDB {len(df)} documentos da coleção '{collection}'.")
        
        database.save_toDatabase(df, config.DATABASE_TABLENAME_VENDAS)
        mark_as_extracted(collection)

    except pymongo.errors.ConnectionFailure:
        print(f"ERRO FATAL: Falha na conexão ao MongoDB em {MONGO_URI}.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()

def extract_WEBSITE_reference_from_mongodb_toDatabase():
    """
    Conecta-se ao MongoDB e carrega o conteúdo da coleção para um Pandas DataFrame.
    """
    
    try:
        # Conexão ao MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client[DB_NAME]
        print("Conexão com o MongoDB bem-sucedida!")

        # Extracção das colections referentes aos comentarios/website
        
        collection = db["website_feedback"]

        # Encontra todos os documentos na coleção
        cursor = collection.find({})
    
        # Converte o cursor do MongoDB diretamente para uma lista de dicionários
        list_of_documents = list(cursor)
    
        if not list_of_documents:
            print(f"A coleção '{collection}' está vazia ou não existe.")
            return pd.DataFrame()

        # Converte ObjectId para string ANTES de criar DataFrame
        for doc in list_of_documents:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        # Converte a lista de dicionários para um DataFrame do Pandas
        df = pd.DataFrame(list_of_documents)
        print(f"Carregados do MongoDB {len(df)} documentos da coleção '{collection}'.")
        
        # Opcional: Remover a coluna '_id' que o MongoDB adiciona     .... Não, eu vou manter!
        #if '_id' in df.columns:
        #    df = df.drop(columns=['_id'])
        
        # tenho de dizer que datahora é um datetime porque vem como string
        #df['data_publicacao'] = df['data_publicacao'].apply(lista_para_datetime)
        
        database.save_toDatabase(df, config.DATABASE_TABLENAME_WEBSITE_FEEDBACK)       # se gravar na mesma tabela "feeds_rss" no Postgres
        mark_as_extracted(collection)

    except pymongo.errors.ConnectionFailure:
        print(f"ERRO FATAL: Falha na conexão ao MongoDB em {MONGO_URI}.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()

def extract_JSON_reference_from_mongodb_toDatabase(source):
    """
    Conecta-se ao MongoDB e carrega o conteúdo da coleção para um Pandas DataFrame.
    """

    try:
        # Conexão ao MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client[DB_NAME]
        print("Conexão com o MongoDB bem-sucedida!")


        # Extracção das colections referentes aos JSON/'ficheiros'

        for sf in source["ficheiros"]:
            collection = db[sf["collection"]]

            # Encontra todos os documentos na coleção
            cursor = collection.find({})
        
            # Converte o cursor do MongoDB diretamente para uma lista de dicionários
            list_of_documents = list(cursor)
        
            if not list_of_documents:
                print(f"A coleção '{sf["collection"]}' está vazia ou não existe.")
                return pd.DataFrame()

            # Converte a lista de dicionários para um DataFrame do Pandas
            df = pd.DataFrame(list_of_documents)
            print(f"Carregados do MongoDB {len(df)} documentos da coleção '{sf["collection"]}'.")
            
            # Opcional: Remover a coluna '_id' que o MongoDB adiciona
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])

            database.save_toDatabase(df, config.DATABASE_TABLENAME_FILE_CSV)
            mark_as_extracted(collection)


        # Extracção das colections referentes aos JSON/'feeds_rss'

        for sr in source["feeds_rss"]:
            sr_collection = db[sr["collection"]]

            # Encontra todos os documentos na coleção
            sr_cursor = sr_collection.find({})
        
            # Converte o cursor do MongoDB diretamente para uma lista de dicionários
            sr_list_of_documents = list(sr_cursor)
        
            if not sr_list_of_documents:
                print(f"A coleção '{sr["collection"]}' está vazia ou não existe.")
                return pd.DataFrame()

            # Converte a lista de dicionários para um DataFrame do Pandas
            df = pd.DataFrame(sr_list_of_documents)
            print(f"Carregados do MongoDB {len(df)} documentos da coleção '{sr["collection"]}'.")
            
            # Opcional: Remover a coluna '_id' que o MongoDB adiciona
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])
            
            # tenho de dizer que datahora é um datetime porque vem como string
            df['data_publicacao'] = df['data_publicacao'].apply(lista_para_datetime)
            
            database.save_toDatabase(df, config.DATABASE_TABLENAME_FEEDS_RSS)       # se gravar na mesma tabela "feeds_rss" no Postgres
            mark_as_extracted(sr_collection)


        # Extracção das colections referentes aos JSON/'ficheiros'

        for sv in source["ficheiro_vendas"]:
            sv_collection = db[sv["collection"]]

            # Encontra todos os documentos na coleção
            sv_cursor = sv_collection.find({})
        
            # Converte o cursor do MongoDB diretamente para uma lista de dicionários
            sv_list_of_documents = list(sv_cursor)
        
            if not sv_list_of_documents:
                print(f"A coleção '{sf["collection"]}' está vazia ou não existe.")
                return pd.DataFrame()
            
            # Converte a lista de dicionários para um DataFrame do Pandas
            df = pd.DataFrame(sv_list_of_documents)
            print(f"Carregados do MongoDB {len(df)} documentos da coleção '{sv["collection"]}'.")
            
            # Opcional: Remover a coluna '_id' que o MongoDB adiciona
            if '_id' in df.columns:
                df = df.drop(columns=['_id'])

            database.save_toDatabase(df, config.DATABASE_TABLENAME_VENDAS)
            mark_as_extracted(collection)

    except pymongo.errors.ConnectionFailure:
        print(f"ERRO FATAL: Falha na conexão ao MongoDB em {MONGO_URI}.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()

def lista_para_datetime(valor):
    """Converte lista [ano, mês, dia, hora, min, seg] para datetime"""
    if isinstance(valor, list):
        try:
            # Converte lista para datetime
            dt = datetime(*valor[:6])
            return dt
        except (ValueError, TypeError, IndexError) as e:
            print(f"Erro ao converter {valor}: {e}")
            return None
    elif isinstance(valor, str):
        return pd.to_datetime(valor)
    elif isinstance(valor, datetime):
        return valor
    return None

def mark_as_extracted(collection):
    """
    Marca registos como extraídos (extracted = True) no MongoDB.    
    Retura --> int: Número de documentos atualizados
    """
    from datetime import datetime
    
    try:
        # Atualiza todos os documentos com extracted = False (ou sem o campo)
        result = collection.update_many(
            {
                "$or": [
                    {"extracted": False},
                    {"extracted": {"$exists": False}}  # Documentos que não têm o campo
                ]
            },
            {
                "$set": {
                    "extracted": True,
                    "extracted_on": datetime.now()
                }
            }
        )
        
        print(f"{result.modified_count} documentos atualizados em MongoDB")
        
        return result.modified_count
        
    except Exception as e:
        print(f"Erro ao atualizar: {e}")
        return 0
            
if __name__ == "__main__":
    try:
        # Carrego o JSON para saber que collection
        #   estou a inserir no MongoDB
        #   não faz sentido descarregar collection que não sei o que lhes vou fazer
        sources_JSON = file.load_dataFrom_JSON(JSON_FILE)
        print(f"Configuração JSON carregada: {len(sources_JSON['ficheiros'])} ficheiros e {len(sources_JSON['feeds_rss'])} feeds.")

        extract_from_mongodb_toDatabase(sources_JSON)
    except FileNotFoundError as e:
        print(e)
    except json.JSONDecodeError:
        print(f"ERRO: Não foi possível analisar o ficheiro '{JSON_FILE}'. Verifique a formatação.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")