# Receita CNPJ Request

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