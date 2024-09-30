import requests
import time
from psycopg2 import errors
from database import fetch_cnpj_list, create_db_connection
from config import load_env, get_db_params
from datetime import datetime
import json
import os

def fetch_data_from_api(cnpj):
    if len(cnpj) != 14 or not cnpj.isdigit():
        print(f"CNPJ '{cnpj}' is not in the correct format. Skipping...")
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
        return datetime.strptime(date_str, '%d/%m/%Y').date()
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
                truncate(data.get('cnpj'), 50), truncate(data.get('ultima_atualizacao'), 50), truncate(data.get('status'), 50), truncate(data.get('email'), 50), truncate(data.get('efr'), 50), truncate(data.get('situacao_especial'), 50),
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

            # Insert into atividades_secundarias table
            for atividade in data.get('atividades_secundarias', []):
                cur.execute("""
                    INSERT INTO atividades_secundarias (empresa_id, code, text)
                    VALUES (%s, %s, %s)
                """, (empresa_id, truncate(atividade['code'], 50), truncate(atividade['text'], 50)))

            # Insert into qsa table
            for socio in data.get('qsa', []):
                cur.execute("""
                    INSERT INTO qsa (empresa_id, nome, qual)
                    VALUES (%s, %s, %s)
                """, (empresa_id, truncate(socio['nome'], 50), truncate(socio['qual'], 50)))

            # Insert into simei table
            cur.execute("""
                INSERT INTO simei (empresa_id, optante, data_opcao, data_exclusao, ultima_atualizacao)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                empresa_id, truncate(data.get('optante_simei'), 50), parse_date(data.get('data_opcao_simei')), parse_date(data.get('data_exclusao_simei')), truncate(data.get('ultima_atualizacao_simei'), 50)
            ))

            conn.commit()
    except errors.UniqueViolation:
        print(f"Chave duplicada encontrada para o CNPJ: {data.get('cnpj')}. Ignorando este registro.")
        conn.rollback()

def load_cnpj_data(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
    return {}

def save_cnpj_data(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

def update_cnpj_data(cnpj_list, cnpj_data):
    for cnpj in cnpj_list:
        cnpj_code = cnpj[0]
        if cnpj_code not in cnpj_data:
            cnpj_data[cnpj_code] = {"requested": False}
    return cnpj_data

if __name__ == "__main__":
    load_env()
    db_params = get_db_params()
    cnpj_list = fetch_cnpj_list(db_params)
    cnpj_data = load_cnpj_data('cnpj_list.json')
    cnpj_data = update_cnpj_data(cnpj_list, cnpj_data)
    conn = create_db_connection()
    
    for cnpj in cnpj_list:
        cnpj_code = cnpj[0]
        if cnpj_data.get(cnpj_code, {}).get("requested"):
            print(f"CNPJ {cnpj_code} já foi consultado. Pulando...")
            continue
        
        data = fetch_data_from_api(cnpj_code)
        if data:
            insert_data(conn, data)
            cnpj_data[cnpj_code] = {"requested": True}
            save_cnpj_data('cnpj_list.json', cnpj_data)
    
    conn.close()