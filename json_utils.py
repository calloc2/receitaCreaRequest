import os
import json

def load_cnpj_data(json_file):
    if os.path.exists(json_file):
        with open(json_file, 'r') as file:
            return json.load(file)
    return {}

def update_cnpj_data(cnpj_list, cnpj_data):
    for cnpj in cnpj_list:
        cnpj_code = cnpj[0]
        if cnpj_code not in cnpj_data:
            cnpj_data[cnpj_code] = {"requested": False}

    cnpj_codes_in_db = {cnpj[0] for cnpj in cnpj_list}
    cnpj_data = {cnpj_code: data for cnpj_code, data in cnpj_data.items() if cnpj_code in cnpj_codes_in_db}

    return cnpj_data

def save_cnpj_data(json_file, cnpj_data):
    with open(json_file, 'w') as file:
        json.dump(cnpj_data, file, indent=4)