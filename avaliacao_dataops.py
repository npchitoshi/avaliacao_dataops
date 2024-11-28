from pymongo import MongoClient
import pandas as pd
import json

data_carros = {'Carro': ['Onix', 'Polo', 'Sandero', 'Fiesta', 'City'],
              'Cor': ['Prata', 'Branco', 'Prata', 'Vermelho', 'Preto'],
              'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda']}
df_carros = pd.DataFrame(data_carros)

data_montadoras = {'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda'],
                 'País': ['EUA', 'Alemanhã', 'França', 'EUA', 'Japão']}
df_montadoras = pd.DataFrame(data_montadoras)


client = MongoClient("mongodb://localhost:27017/")
db = client['avaliacao_dataops']

# INSERT carros
collection = db['Carros']
result_carros = collection.insert_many(df_carros.to_dict(orient='records'))

# INSERT montadoras
collection = db['Montadoras']
result_montadoras = collection.insert_many(df_montadoras.to_dict(orient='records'))

# Primeiro agrupamento
pipeline = [
    {
        '$lookup': {
            'from': 'Montadoras',
            'localField': 'Montadora',
            'foreignField': 'Montadora',
            'as': 'Montadoras'
        }
    },
    {
        '$project': {
            '_id': 1,
            'Carro': 1,
            'Cor': 1,
            'Montadora': 1,
            'Montadoras': { '$size': '$Montadoras' },
            'País': {'$arrayElemAt': ['$Montadoras.País', 0]}
        }
    }
]
result = list(db['Carros'].aggregate(pipeline))
df = pd.DataFrame(result)
print(df)

# Segundos agrupamento
pipeline = [
    {
        '$lookup': {
            'from': 'Montadoras',
            'localField': 'Montadora',
            'foreignField': 'Montadora',
            'as': 'Montadoras'
        }
    },
    {
        '$unwind': '$Montadoras'
    },
    {
        '$group': {
            '_id': '$Montadoras.País',
            'Carros': {'$push': '$Carro'}
        }
    },
    {
        '$project': {
            '_id': 1,
            'Carros': {'$size': '$Carros'}
        }
    }
]
result = list(db['Carros'].aggregate(pipeline))
df = pd.DataFrame(result)
print(df)

# Salvando agregação final em arquivo .js
result_json = df.to_dict(orient='records')
js_content = f"const countryCount = {json.dumps(result_json, indent=4)};"
with open('country_count.js', 'w') as js_file:
    js_file.write(js_content)

client.close()