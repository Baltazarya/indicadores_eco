import pandas as pd
import requests
import json 
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()

# Funções a serem utilizadas no código

def pegar_dados(codigo_serie, dataInicial, dataFinal):
    """
    Função para pegar dados de uma série do SGS do Banco Central do Brasil.
    
    Parâmetros:
    codigo_serie (int): Código da série/indicador a ser consultada.
    dataInicial (str): Data inicial no formato 'dd/mm/yyyy'.
    dataFinal (str): Data final no formato 'dd/mm/yyyy'.
    
    Retorna:
    pd.DataFrame: DataFrame com os dados da série.
    """
    # URL da API do SGS
    URL = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json&dataInicial={dataInicial}&dataFinal={dataFinal}"

    # Fazendo a requisição à API
    response = requests.get(URL)
    
    # Verificando se a requisição foi bem sucedida
    if response.status_code == 200:
        dados = json.loads(response.text)
        df = pd.DataFrame(dados)
        return df
    else:
        print(f"Erro ao acessar a API: {response.status_code}")
        return None
    
def coletar_multiplas_series(ind_dict, dataInicial, dataFinal):
    """
    Coleta múltiplas séries do SGS e retorna um único DataFrame com coluna 'serie'.
    
    Parâmetros:
    - ind_dict (dict): dicionário com {codigo: nome_serie}
    - dataInicial, dataFinal (str): datas no formato 'dd/mm/yyyy'
    
    Retorna:
    - pd.DataFrame: 'data' e uma coluna para cada série
    """
    df_final = pd.DataFrame()

    for codigo, nome in ind_dict.items():
        print(f"Coletando série {nome} ({codigo})...")
        df = pegar_dados(codigo, dataInicial, dataFinal)
        if df is not None:
            df = df.rename(columns={'valor': nome})
            if df_final.empty:
                df_final = df
            else:
                df_final = pd.merge(df_final, df[['data', nome]], on='data', how='outer')

    df_final = df_final.sort_values('data').reset_index(drop=True)
    return df_final   

def carregar_dados_postgresql(df, nome_tabela, usuario, senha, host, porta, banco, substituir=True):
    #Função para carregar dados em um banco de dados PostgreSQL
    modo = 'replace' if substituir else 'append'
    conn_str = f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}'
    engine = create_engine(conn_str)
    df.to_sql(nome_tabela, con=engine, index=False, if_exists=modo)
    print(f"✅ Dados carregados na tabela '{nome_tabela}' no banco '{banco}' com sucesso.")


"""
series sendo usadas para analise:
    24369: "Taxa_Desemprego",
    28766: "Pop_Desocupada",
    28765: "Pop_Ocupada",
    1619: "Rendimento_Medio",
    4380: "PIB",
    432: "Selic",
    433: "IPCA",
    25492: "Confiança_Consumidor",
    21859: "Producao_Industrial"

Coletando dados de 01/01/2015 a 01/01/2025

Transformando a coluna 'data' para o formato datetime e ordenando os dados por data

Carregando os dados no banco de dados PostgreSQL

"""

series = {
    24369: "Taxa_Desemprego",
    28766: "Pop_Desocupada",
    28765: "Pop_Ocupada",
    1619: "Rendimento_Medio",
    4380: "PIB",
    432: "Selic",
    433: "IPCA",
    25492: "Confiança_Consumidor",
    21859: "Producao_Industrial"
}
# Extract
df = coletar_multiplas_series(series, "01/01/2015", "01/01/2025")

# Transform
df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
df = df.sort_values('data').reset_index(drop=True)

df = df.dropna()
df = df.rename(columns={'data': 'Data'})
df.columns = df.columns.str.lower().str.replace(' ', '_')
# Converte todas as colunas numéricas, exceto 'data'
for col in df.columns:
    if col != 'data':
        df[col] = pd.to_numeric(df[col], errors='coerce')


# Load
df.to_csv("dados_sgs.csv", index=False)


carregar_dados_postgresql(
        df=df,
        nome_tabela='indicadores_desemprego',
        usuario=os.getenv('DB_USER'),
        senha=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        porta=os.getenv('DB_PORT'),
        banco=os.getenv('DB_NAME')
)