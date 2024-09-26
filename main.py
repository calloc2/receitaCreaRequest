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

cur.close()
conn.close()

for cnpj in cnpj_list:
    cnpj_code = cnpj[0]
    print(cnpj_code)
    url = "https://receitaws.com.br/v1/cnpj/{cnpj_code}"
    print(url)
    response = requests.get(url)
    print(response.text)
    
    time.sleep(20)