import requests
import time
from psycopg2 import errors
from database import fetch_cnpj_list, create_db_connection, fetch_existing_cnpjs
from config import load_env, get_db_params
from datetime import datetime
import json
import os
import re
from json_utils import load_cnpj_data, save_cnpj_data, update_cnpj_data

def fetch_data_from_api(cnpj, cnpj_data):
    if len(cnpj) != 14 or not cnpj.isdigit():
        print(f"CNPJ '{cnpj}' is not in the correct format. Skipping...")
        return None

    if cnpj_data.get(cnpj, {}).get("requested"):
        print(f"CNPJ {cnpj} já foi consultado. Pulando...")
        return None

    url = f"https://receitaws.com.br/v1/cnpj/{cnpj}"
    while True:
        response = requests.get(url)
        
        if response.status_code == 429:
            print(f"Excedido o número de requisições para o CNPJ: {cnpj}. Esperando 60 segundos antes de tentar novamente...")
            time.sleep(60)
            continue
        
        try:
            data = response.json()
            return data
        except requests.exceptions.JSONDecodeError:
            print(f"Erro ao decodificar JSON para o CNPJ: {cnpj}. Retrying...")
            time.sleep(20)
            continue

def parse_date(date_str):
    if date_str:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return None
    return None

def truncate(value, max_length):
    if value and len(value) > max_length:
        return value[:max_length]
    return value

def insert_data(conn, data):
    if data is None:
        return
    
    try:
        with conn.cursor() as cur:
            # Remove punctuation from CNPJ
            cnpj = re.sub(r'\D', '', data.get('cnpj', ''))
            
            # Insert into empresa table
            cur.execute("""
                INSERT INTO empresa (
                    abertura, situacao, tipo, nome, fantasia, porte, natureza_juridica, logradouro, numero, complemento,
                    municipio, bairro, uf, cep, telefone, data_situacao, motivo_situacao, cnpj, ultima_atualizacao, status,
                    email, efr, situacao_especial, data_situacao_especial, capital_social, simples_optante, simples_data_opcao,
                    simples_data_exclusao, simples_ultima_atualizacao, billing_free, billing_database
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                truncate(data.get('abertura'), 50), truncate(data.get('situacao'), 50), truncate(data.get('tipo'), 50), truncate(data.get('nome'), 50), truncate(data.get('fantasia'), 50), truncate(data.get('porte'), 50),
                truncate(data.get('natureza_juridica'), 50), truncate(data.get('logradouro'), 50), truncate(data.get('numero'), 50), truncate(data.get('complemento'), 50), truncate(data.get('municipio'), 50),
                truncate(data.get('bairro'), 50), truncate(data.get('uf'), 50), truncate(data.get('cep'), 50), truncate(data.get('telefone'), 50), parse_date(data.get('data_situacao')), truncate(data.get('motivo_situacao'), 50),
                cnpj, truncate(data.get('ultima_atualizacao'), 50), truncate(data.get('status'), 50), truncate(data.get('email'), 50), truncate(data.get('efr'), 50), truncate(data.get('situacao_especial'), 50),
                parse_date(data.get('data_situacao_especial')), truncate(data.get('capital_social'), 50), truncate(data.get('simples_optante'), 50), parse_date(data.get('simples_data_opcao')),
                parse_date(data.get('simples_data_exclusao')), truncate(data.get('simples_ultima_atualizacao'), 50), truncate(data.get('billing_free'), 50), truncate(data.get('billing_database'), 50)
            ))
            empresa_id = cur.fetchone()[0]

            # Insert into atividade_principal table
            for atividade in data.get('atividade_principal', []):
                cur.execute("""
                    INSERT INTO atividade_principal (empresa_id, code, text)
                    VALUES (%s, %s, %s)
                """, (empresa_id, truncate(atividade['code'], 50), truncate(atividade['text'], 50)))

            # Insert into atividade_secundaria table
            for atividade in data.get('atividade_secundaria', []):
                cur.execute("""
                    INSERT INTO atividade_secundaria (empresa_id, code, text)
                    VALUES (%s, %s, %s)
                """, (empresa_id, truncate(atividade['code'], 50), truncate(atividade['text'], 50)))

            conn.commit()
            print(f"CNPJ {cnpj} inserido com sucesso.")
    except errors.UniqueViolation:
        print(f"CNPJ {cnpj} já existe no banco de dados. Pulando...")

def load_cnpj_data(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            try:
                return json.load(file)
            except json.JSONDecodeError:
                print(f"JSON file {file_path} is empty or corrupted. Fetching data from the database...")
                return None
    return None

def save_cnpj_data(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

if __name__ == "__main__":
    load_env()
    db_params = get_db_params()
    cnpj_list = fetch_cnpj_list(db_params)
    cnpj_data = load_cnpj_data('cnpj_list.json')
    
    conn = create_db_connection()
    existing_cnpjs = fetch_existing_cnpjs(conn)
    
    if cnpj_data is None:
        cnpj_data = update_cnpj_data(cnpj_list, {})
    else:
        cnpj_data = update_cnpj_data(cnpj_list, cnpj_data)
    
    for cnpj in existing_cnpjs:
        if cnpj in cnpj_data:
            cnpj_data[cnpj]["requested"] = True
    
    save_cnpj_data('cnpj_list.json', cnpj_data)
    
    for cnpj in cnpj_list:
        cnpj_code = cnpj[0]
        if cnpj_data.get(cnpj_code, {}).get("requested"):
            print(f"CNPJ {cnpj_code} já foi consultado. Pulando...")
            continue
        
        data = fetch_data_from_api(cnpj_code, cnpj_data)
        if data:
            insert_data(conn, data)
            cnpj_data[cnpj_code] = {"requested": True}
            save_cnpj_data('cnpj_list.json', cnpj_data)
        
    conn.close()