import requests
import time
from psycopg2 import errors
from database import fetch_cnpj_list, create_db_connection
from config import*
from datetime import datetime

def fetch_data_from_api(cnpj):
    url = f"https://receitaws.com.br/v1/cnpj/{cnpj}"
    response = requests.get(url)
    
    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"Erro ao decodificar JSON para o CNPJ: {cnpj}")
        return None
    
    return data

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

if __name__ == "__main__":
    load_env()
    db_params = get_db_params()
    cnpj_list = fetch_cnpj_list(db_params)
    conn = create_db_connection()
    for cnpj in cnpj_list:
        data = fetch_data_from_api(cnpj[0])
        if data:
            insert_data(conn, data)
    conn.close()