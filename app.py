import threading
import time
from insert_data import fetch_data_from_api, insert_data
from database import create_db_connection, fetch_cnpj_list
from config import load_env, get_db_params
from tqdm import tqdm

process_thread = None
stop_event = threading.Event()

def start():
    global process_thread, stop_event
    if process_thread is None or not process_thread.is_alive():
        stop_event.clear()
        process_thread = threading.Thread(target=process_cnpjs)
        process_thread.start()
        return {'status': 'started'}
    else:
        stop_event.set()
        process_thread.join()
        return {'status': 'stopped'}

def process_cnpjs():
    load_env()
    db_params = get_db_params()
    cnpj_list = fetch_cnpj_list(db_params)
    conn = create_db_connection()
    total_cnpjs = len(cnpj_list)
    consulted_cnpjs = 0

    with tqdm(total=total_cnpjs, desc="Inserindo CNPJs", unit="CNPJ") as pbar:
        for cnpj in cnpj_list:
            if stop_event.is_set():
                break

            cnpj_code = cnpj[0]
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM empresa WHERE cnpj = %s", (cnpj_code,))
                if cur.fetchone():
                    print(f"CNPJ {cnpj_code} já existe no banco de dados. Pulando...")
                    pbar.update(1)
                    consulted_cnpjs += 1
                    continue

            data = fetch_data_from_api(cnpj_code)
            if data:
                insert_data(conn, data)
                time.sleep(20)
            pbar.update(1)
            consulted_cnpjs += 1

    conn.close()

if __name__ == '__main__':
    start()