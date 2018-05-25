from pymongo import MongoClient

def mongodb_connect(db_name, collection):
    client = MongoClient('mongodb://<username>:<password>@<host>:<port>')
    db = client[db_name]
    collection = db[collection]
    return collection