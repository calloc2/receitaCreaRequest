
# Skip to your language / Pule para sua linguagem:

- [English](#english)
- [Português](#português)

# English

## CNPJ Request Project

This project makes requests to the Federal Revenue API using a list of CNPJs stored in a PostgreSQL database. The CNPJ list is maintained in a local JSON file, which is updated as needed.

## Project Structure

```
config.py
database.py
json_utils.py
main.py
request_handler.py
```

## Configuration

### .env File

Create a `.env` file in the project root with the following environment variables:

```
DB_NAME=DATABASE_NAME
DB_USER=USER
DB_PASSWORD=PASSWORD
DB_HOST=127.0.0.1
DB_PORT=5432

DB_NAME_LOCAL=LOCAL_DATABASE_NAME
DB_USER_LOCAL=USER
DB_PASSWORD_LOCAL=PASSWORD
DB_HOST_LOCAL=127.0.0.1
DB_PORT_LOCAL=5432
```

### Dependencies

Install the required dependencies using `pip`:

```sh
pip install -r requirements.txt
```

### File Structure

- **`config.py`**: Loads environment variables and provides database connection parameters.
- **`database.py`**: Contains the function to fetch the CNPJ list from the database.
- **`json_utils.py`**: Contains functions to load, update, and save data in the JSON file.
- **`request_handler.py`**: Contains the function to make requests to the Federal Revenue API.
- **`main.py`**: The main file that orchestrates the execution of the script.

## Usage

### Running the Script

To run the script, simply execute the `main.py` file:

```sh
python main.py
```

# Português

## Projeto de Requisição CNPJ

Este projeto realiza requisições para a API da Receita Federal utilizando uma lista de CNPJs armazenada em um banco de dados PostgreSQL. A lista de CNPJs é mantida em um arquivo JSON local, que é atualizado conforme necessário.

## Estrutura do Projeto

```
config.py
database.py
json_utils.py
main.py
request_handler.py
```

## Configuração

### Arquivo .env

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis de ambiente:

```
DB_NAME=NOME_BANCO_DE_DADOS
DB_USER=USUARIO
DB_PASSWORD=SENHA
DB_HOST=127.0.0.1
DB_PORT=5432

DB_NAME_LOCAL=NOME_BANCO_DE_DADOS
DB_USER_LOCAL=USUARIO
DB_PASSWORD_LOCAL=SENHA
DB_HOST_LOCAL=127.0.0.1
DB_PORT_LOCAL=5432
```

### Dependências

Instale as dependências necessárias utilizando `pip`:

```sh
pip install -r requirements.txt
```

### Estrutura dos Arquivos

- **`config.py`**: Carrega as variáveis de ambiente e fornece os parâmetros de conexão com o banco de dados.
- **`database.py`**: Contém a função para buscar a lista de CNPJs do banco de dados.
- **`json_utils.py`**: Contém funções para carregar, atualizar e salvar dados no arquivo JSON.
- **`request_handler.py`**: Contém a função para realizar as requisições à API da Receita Federal.
- **`main.py`**: Arquivo principal que orquestra a execução do script.

## Uso

### Executando o Script

Para executar o script, basta rodar o arquivo `main.py`:

```sh
python main.py
```
