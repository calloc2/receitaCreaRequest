import os
import time
import requests
import psycopg2
import json
from dotenv import load_dotenv

load_dotenv()

db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

json_file = 'cnpj_list.json'

if os.path.exists(json_file):
    with open(json_file, 'r') as file:
        cnpj_data = json.load(file)
else:
    cnpj_data = {}

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

cur.execute("SELECT cnpj FROM tb_empresa")
cnpj_list = cur.fetchall()
cur.close()
conn.close()

for cnpj in cnpj_list:
    cnpj_code = cnpj[0]
    if cnpj_code not in cnpj_data:
        cnpj_data[cnpj_code] = {"requested": False}

cnpj_codes_in_db = {cnpj[0] for cnpj in cnpj_list}
cnpj_data = {cnpj_code: data for cnpj_code, data in cnpj_data.items() if cnpj_code in cnpj_codes_in_db}

with open(json_file, 'w') as file:
    json.dump(cnpj_data, file, indent=4)

for cnpj_code, data in cnpj_data.items():
    if data["requested"]:
        continue
    print(cnpj_code)
    url = f"https://receitaws.com.br/v1/cnpj/{cnpj_code}"
    print(url)
    response = requests.get(url)
    print(response.text)
    
    cnpj_data[cnpj_code]["requested"] = True
    
    with open(json_file, 'w') as file:
        json.dump(cnpj_data, file, indent=4)
    
    time.sleep(20)