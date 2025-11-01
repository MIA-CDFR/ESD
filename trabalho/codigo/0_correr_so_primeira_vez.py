from configs import config as config
import _setup_database
import _delete_mongodb

if __name__ == "__main__":
    
    # Começo por APAGAR o MONGODB
    _delete_mongodb.limpar_mongo_esd_wine()

    # Depois por REFAZER todo o esquema da base de dados
    _setup_database.run_schema()