import pandas as pd
import os

# Caminho onde estão os CSVs
pasta_csv = 'dados/'  # ajuste para o caminho real
arquivos = os.listdir(pasta_csv)

for nome_arquivo in arquivos:
    if nome_arquivo.endswith('.csv'):
        print(f'\nArquivo: {nome_arquivo}')
        df = pd.read_csv(os.path.join(pasta_csv, nome_arquivo), sep=';', encoding='utf-8')
        print(df.columns.tolist())  # lista as colunas
        print(df.head(1))  # mostra só a primeira linha de dados
