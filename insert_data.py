import requests
import psycopg2
from database import*
from config import load_env, get_db_params, get_local_db_params


def insert_data(conn, data):
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
            data.get('abertura'), data.get('situacao'), data.get('tipo'), data.get('nome'), data.get('fantasia'), data.get('porte'),
            data.get('natureza_juridica'), data.get('logradouro'), data.get('numero'), data.get('complemento'), data.get('municipio'),
            data.get('bairro'), data.get('uf'), data.get('cep'), data.get('telefone'), data.get('data_situacao'), data.get('motivo_situacao'),
            data.get('cnpj'), data.get('ultima_atualizacao'), data.get('status'), data.get('email'), data.get('efr'), data.get('situacao_especial'),
            data.get('data_situacao_especial'), data.get('capital_social'), data.get('simples_optante'), data.get('simples_data_opcao'),
            data.get('simples_data_exclusao'), data.get('simples_ultima_atualizacao'), data.get('billing_free'), data.get('billing_database')
        ))
        empresa_id = cur.fetchone()[0]

        # Insert into atividade_principal table
        for atividade in data.get('atividade_principal', []):
            cur.execute("""
                INSERT INTO atividade_principal (empresa_id, code, text)
                VALUES (%s, %s, %s)
            """, (empresa_id, atividade['code'], atividade['text']))

        # Insert into atividades_secundarias table
        for atividade in data.get('atividades_secundarias', []):
            cur.execute("""
                INSERT INTO atividades_secundarias (empresa_id, code, text)
                VALUES (%s, %s, %s)
            """, (empresa_id, atividade['code'], atividade['text']))

        # Insert into qsa table
        for socio in data.get('qsa', []):
            cur.execute("""
                INSERT INTO qsa (empresa_id, nome, qual)
                VALUES (%s, %s, %s)
            """, (empresa_id, socio['nome'], socio['qual']))

        # Insert into simei table
        cur.execute("""
            INSERT INTO simei (empresa_id, optante, data_opcao, data_exclusao, ultima_atualizacao)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            empresa_id, data.get('optante_simei'), data.get('data_opcao_simei'), data.get('data_exclusao_simei'), data.get('ultima_atualizacao_simei')
        ))

        conn.commit()

    
cnpj = get_db_params()
data = fetch_cnpj_list(cnpj)
conn = create_db_connection()
insert_data(conn, data)
conn.close()