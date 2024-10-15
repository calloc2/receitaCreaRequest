import psycopg2
from config import load_env, get_local_db_params

def fetch_cnpj_list(db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute("""
        SELECT te.cnpj 
        FROM tb_empresa te
        INNER JOIN tb_naturezajuridica tn ON tn.id = te.naturezajuridica_id 
        WHERE tn.ativo = true
    """)
    cnpj_list = cur.fetchall()
    cur.close()
    conn.close()
    return cnpj_list

def create_db_connection():
    load_env()
    db_params = get_local_db_params()
    conn = psycopg2.connect(**db_params)
    return conn

def fetch_existing_cnpjs(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT cnpj FROM empresa")
        return [row[0] for row in cur.fetchall()]