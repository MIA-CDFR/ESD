import pymongo
from pymongo import MongoClient
import json
from configs import config as config, mongodb as mongo
from scripts import files as file
import database
import _setup_database
import _delete_mongodb
from datetime import datetime

# --- Configurações do Ficheiro ---
JSON_FILE = config.FILENAME_SOURCES_TO_IMPORT           # O nome do seu ficheiro JSON, e.g., "sources.json"
DIR_SOURCES = config.DIR_SOURCES                        # O diretório base para procurar os CSVs (deve ser um objeto Path)

# --- Configurações do MongoDB ---
MONGO_URI = mongo.MONGO_URI
DB_NAME = mongo.DB_NAME

# --- Funções de Ingestão (Principal) ---

def ingest_to_mongodb(source):
    """Faz a leitura dos dados (CSVs e Feeds) e a inserção no MongoDB."""
    
    try:
        # Conexão ao MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client[DB_NAME]
        print("Conexão com o MongoDB bem-sucedida!")

        # Ingestão de ficheiros CSV
        
        # for source_file_name, target_collection in config_data["ficheiros"].items():
        for sf in source["ficheiros"]:
            sf_filename = sf["nome"]
            sf_collection = sf["collection"]
            
            # Constrói o caminho completo usando o Pathlib (DIR_SOURCES deve ser um objeto Path)
            source_file_path = DIR_SOURCES / sf_filename

            # Chama a função load_csv do seu módulo files.py
            #   Devolve um DataFrame
            df = file.load_csv(source_file_path)

            # tenho de converter para list porque é formato aceite pelo MongoDB
            #   E no caso, df = file.load_csv(source_file_path) é um DataFrame
            documents = df.to_dict('records')

            # crio 2 campos para ficar com o estado (extracted rue/false)
            #       e data do extratec_on
            for d in documents:
                d["extracted"] = False
            
            if documents:
                # Insere na coleção designada
                collection = db[sf_collection]                
                result = collection.insert_many(documents)
                print(f"Inseridos {len(result.inserted_ids)} documentos na coleção '{sf_collection}'.")

        # Ingestão de Feeds RSS
        
        all_rss_documents = []

        for sr in source["feeds_rss"]:
            sr_filename = sr["nome"]
            sr_collection = sr["collection"]

            # Descarrega o feed e obtém a lista de artigos (função do seu files.py)
            articles = file.fetch_rss_feed(sr)

            # crio 2 campos para ficar com o estado (extracted rue/false)
            #       e data do extratec_on
            for a in articles:
                a["extracted"] = False

                # 1. Obter a data e hora atual (com fuso horário ou sem) 
                #   Exemplo: 2025-10-31 00:05:24.130000
                datahora = datetime.now()
                # 2. Aplicar o método strftime() com o formato desejado
                #   Exemplo: 2025-10-31 00:11:26
                a["datahora"] = datahora.strftime("%Y-%m-%d %H:%M:%S")

            all_rss_documents.extend(articles)

            if all_rss_documents:
                rss_collection = db[sr_collection]
                rss_result = rss_collection.insert_many(all_rss_documents)
                all_rss_documents.clear()   # Limpo array para não acumular
                print(f"Inseridos {len(rss_result.inserted_ids)} artigos na coleção '{sr_collection}'.")
            else:
                print("Nenhum artigo de feed RSS foi descarregado ou inserido.")

        print("Processo de Ingestão concluído com sucesso!")

    except pymongo.errors.ConnectionFailure as e:
        print(f"Não foi possível conectar ao MongoDB. Verifique se o serviço está ativo em {MONGO_URI}")
        print(f"Detalhes: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a ingestão: {e}")
    finally:
        # Fechar a conexão
        if 'client' in locals() and client:
            client.close()
            print("Conexão ao MongoDB fechada.")

if __name__ == "__main__":
    try:
        # Carrego o JSON
        sources_JSON = file.load_dataFrom_JSON(JSON_FILE)
        print(f"Configuração JSON carregada.")

        # Faço a ingestão de todos os ficheiros e feeds RSS definidos no JSON
        ingest_to_mongodb(sources_JSON)
    except FileNotFoundError as e:
        print(e)
    except json.JSONDecodeError:
        print(f"ERRO: Não foi possível analisar o ficheiro '{JSON_FILE}'. Verifique a formatação.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")