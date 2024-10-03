-- Tabela principal para armazenar informações da empresa
CREATE TABLE empresa (
    id SERIAL PRIMARY KEY,
    abertura DATE,
    situacao VARCHAR(50),
    tipo VARCHAR(50),
    nome VARCHAR(255),
    fantasia VARCHAR(255),
    porte VARCHAR(50),
    natureza_juridica VARCHAR(255),
    logradouro VARCHAR(255),
    numero VARCHAR(50),
    complemento VARCHAR(50),
    municipio VARCHAR(100),
    bairro VARCHAR(100),
    uf VARCHAR(2),
    cep VARCHAR(10),
    telefone VARCHAR(50),
    data_situacao DATE,
    motivo_situacao VARCHAR(255),
    cnpj VARCHAR(18) UNIQUE,
    ultima_atualizacao TIMESTAMP,
    status VARCHAR(20),
    email VARCHAR(255),
    efr VARCHAR(255),
    situacao_especial VARCHAR(255),
    data_situacao_especial DATE,
    capital_social DECIMAL(15, 2),
    simples_optante BOOLEAN,
    simples_data_opcao DATE,
    simples_data_exclusao DATE,
    simples_ultima_atualizacao TIMESTAMP,
    billing_free BOOLEAN,
    billing_database BOOLEAN
);

-- Tabela para armazenar os sócios (QSA)
CREATE TABLE qsa (
    id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresa(id) ON DELETE CASCADE,
    nome VARCHAR(255),
    qual VARCHAR(100)
);

-- Tabela para armazenar atividades principais
CREATE TABLE atividade_principal (
    id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresa(id) ON DELETE CASCADE,
    code VARCHAR(10),
    text VARCHAR(255)
);

-- Tabela para armazenar atividades secundárias
CREATE TABLE atividades_secundarias (
    id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresa(id) ON DELETE CASCADE,
    code VARCHAR(10),
    text VARCHAR(255)
);

-- Tabela para armazenar informações do SIMEI
CREATE TABLE simei (
    id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresa(id) ON DELETE CASCADE,
    optante BOOLEAN,
    data_opcao DATE,
    data_exclusao DATE,
    ultima_atualizacao TIMESTAMP
);

