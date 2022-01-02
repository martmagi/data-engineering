import pandas as pd
import pymongo

MONGO_USER = 'data-engineering-g12'
MONGO_SECRET = 'mkj2xap*CJB1btn2gcz'
MONGO_URL = 'data-engineering-g12.wvade.mongodb.net'
MONGO_SCHEMA = 'myFirstDatabase'
MONGO_CONNECTION_STRING = f'mongodb+srv://{MONGO_USER}:{MONGO_SECRET}@{MONGO_URL}/{MONGO_SCHEMA}?retryWrites=true&w=majority'


def upload_to_mongo(transformed_file_path: str):
    client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
    db = client.memesDB

    df = pd.read_json(f"{transformed_file_path}")
    results_list = df.to_dict(orient='records')
    db.memes.insert_many(results_list)
