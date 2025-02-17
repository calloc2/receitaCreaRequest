import requests
import time
from psycopg2 import errors, OperationalError
from database import fetch_cnpj_list, create_db_connection, fetch_existing_cnpjs
from config import load_env, get_db_params
from datetime import datetime
import json
import os
import re
from tqdm import tqdm

def fetch_data_from_api(cnpj):
    if len(cnpj) != 14 or not cnpj.isdigit():
        print(f"CNPJ '{cnpj}' não está no formato correto. Pulando...")
        return None

    url = f"https://receitaws.com.br/v1/cnpj/{cnpj}"
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(url)
            
            if response.status_code == 429:
                print(f"Excedido o número de requisições para o CNPJ: {cnpj}. Esperando 60 segundos antes de tentar novamente...")
                time.sleep(60)
                retries += 1
                continue
            
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a requisição para o CNPJ: {cnpj}. Erro: {e}. Tentando novamente em 60 segundos...")
            time.sleep(60)
            retries += 1
        except requests.exceptions.JSONDecodeError:
            print(f"Erro ao decodificar JSON para o CNPJ: {cnpj}. Tentando novamente em 20 segundos...")
            time.sleep(20)
            retries += 1

    print(f"Falha ao obter dados para o CNPJ: {cnpj} após {max_retries} tentativas. Pulando...")
    return None

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
            cnpj = re.sub(r'\D', '', data.get('cnpj', ''))
            
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

            for atividade in data.get('atividade_principal', []):
                cur.execute("""
                    INSERT INTO atividade_principal (empresa_id, code, text)
                    VALUES (%s, %s, %s)
                """, (empresa_id, truncate(atividade['code'], 50), truncate(atividade['text'], 50)))

            for atividade in data.get('atividades_secundarias', []):
                cur.execute("""
                    INSERT INTO atividades_secundarias (empresa_id, code, text)
                    VALUES (%s, %s, %s)
                """, (empresa_id, truncate(atividade['code'], 50), truncate(atividade['text'], 50)))

            conn.commit()
            print(f"CNPJ {cnpj} inserido com sucesso.")
    except errors.UniqueViolation:
        print(f"CNPJ {cnpj} já existe no banco de dados. Pulando...")
        conn.rollback()
    except Exception as e:
        print(f"Erro ao inserir CNPJ {cnpj}: {e}")
        conn.rollback()

if __name__ == "__main__":
    load_env()
    db_params = get_db_params()
    
    try:
        cnpj_list = fetch_cnpj_list(db_params)
    except OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        exit(1)
    
    conn = create_db_connection()
    
    total_cnpjs = len(cnpj_list)
    consulted_cnpjs = 0
    
    start_time = datetime.now()
    print(f"Início do processamento: {start_time}")

    try:
        with tqdm(total=total_cnpjs, desc="Inserindo CNPJs", unit="CNPJ") as pbar:
            for cnpj in cnpj_list:
                cnpj_code = cnpj[0]
                
                # Verificar se o CNPJ já existe no banco de dados
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM empresa WHERE cnpj = %s", (cnpj_code,))
                    if cur.fetchone():
                        print(f"CNPJ {cnpj_code} já existe no banco de dados. Pulando...")
                        pbar.update(1)
                        consulted_cnpjs += 1
                        print(f"Consultados: {consulted_cnpjs}, Restantes: {total_cnpjs - consulted_cnpjs}")
                        continue
                
                data = fetch_data_from_api(cnpj_code)
                if data:
                    try:
                        insert_data(conn, data)
                    except Exception as e:
                        print(f"Erro ao inserir dados para o CNPJ {cnpj_code}: {e}")
                time.sleep(20)
                pbar.update(1)
                consulted_cnpjs += 1
                print(f"Consultados: {consulted_cnpjs}, Restantes: {total_cnpjs - consulted_cnpjs}")
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
    finally:
        conn.close()
    
    end_time = datetime.now()
    print(f"Término do processamento: {end_time}")
    duration = end_time - start_time
    print(f"Duração total do processamento: {duration}")