import time
import requests
from json_utils import save_cnpj_data

def make_requests(cnpj_data, json_file):
    for cnpj_code, data in cnpj_data.items():
        if data["requested"]:
            continue
        print(cnpj_code)
        url = f"https://receitaws.com.br/v1/cnpj/{cnpj_code}"
        print(url)
        response = requests.get(url)
        print(response.text)
        
        cnpj_data[cnpj_code]["requested"] = True
        save_cnpj_data(json_file, cnpj_data)
        
        time.sleep(20)