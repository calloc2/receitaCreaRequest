import psycopg2

def fetch_cnpj_list(db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute("SELECT cnpj FROM tb_empresa")
    cnpj_list = cur.fetchall()
    cur.close()
    conn.close()
    return cnpj_list