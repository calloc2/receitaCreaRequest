import os
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

cur.execute("SELECT cnpj FROM tb_empresa")
cnpj_list = cur.fetchall()
print("CNPJ List:", cnpj_list)
cur.close()
conn.close()

for cnpj in cnpj_list:
    cnpj_code = cnpj[0]
    if not cnpj_code:
        continue
    print(cnpj_code)
    url = f"https://receitaws.com.br/v1/cnpj/{cnpj_code}"
    print(url)
    response = requests.get(url)
    print(response.text)
    
    time.sleep(20)