from config import load_env, get_db_params
from database import fetch_cnpj_list
from json_utils import load_cnpj_data, update_cnpj_data, save_cnpj_data
from request_handler import make_requests

def main():
    load_env()
    db_params = get_db_params()
    json_file = 'cnpj_list.json'
    
    cnpj_list = fetch_cnpj_list(db_params)
    cnpj_data = load_cnpj_data(json_file)
    cnpj_data = update_cnpj_data(cnpj_list, cnpj_data)
    save_cnpj_data(json_file, cnpj_data)
    make_requests(cnpj_data, json_file)

if __name__ == "__main__":
    main()