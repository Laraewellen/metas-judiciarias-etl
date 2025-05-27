
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional
from tqdm import tqdm
from rich.logging import RichHandler
import logging

logging.basicConfig(level="INFO", format="[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S", handlers=[RichHandler()])
log = logging.getLogger("rich")

PASTA_CSV = 'dados'
PASTA_RESULTADOS = 'resultados_versao_NP'
ARQUIVO_RESUMO = os.path.join(PASTA_RESULTADOS, 'ResumoMetas.csv')
ARQUIVO_CONSOLIDADO = os.path.join(PASTA_RESULTADOS, 'Consolidado.csv')
GRAFICO_META1 = os.path.join(PASTA_RESULTADOS, 'grafico_meta1.png')

fatores_metas_por_ramo = {
    'Justiça Estadual': {
        '2a': 1000/8, '2b': 1000/9, '2c': 1000/9.5, '2ant': 100,
        '4a': 1000/6.5, '4b': 100, '6': 100,
        '7a': 1000/5, '7b': 1000/5, '8a': 1000/7.5, '8b': 1000/9,
        '10a': 1000/9, '10b': 1000/10
    },
    'Justiça do Trabalho': {
        '2a': 1000/9.4, '2ant': 100, '4a': 1000/7, '4b': 100
    },
    'Justiça Federal': {
        '2a': 1000/8.5, '2b': 100, '2ant': 100, '4a': 1000/7, '4b': 100,
        '6': 1000/3.5, '7a': 1000/3.5, '7b': 1000/3.5, '8a': 1000/7.5, '8b': 1000/9,
        '10a': 100
    },
    'Justiça Militar da União': {
        '2a': 1000/9.5, '2b': 1000/9.9, '2ant': 100,
        '4a': 1000/9.5, '4b': 1000/9.9
    },
    'Justiça Militar Estadual': {
        '2a': 1000/9, '2b': 1000/9.5, '2ant': 100,
        '4a': 1000/9.5, '4b': 1000/9.9
    },
    'Tribunal Superior Eleitoral': {
        '2a': 1000/7.0, '2b': 1000/9.9, '2ant': 100,
        '4a': 1000/9, '4b': 1000/5
    },
    'Tribunal Superior do Trabalho': {
        '2a': 1000/8.5, '2b': 1000/9.9, '2ant': 100, '4a': 1000/7, '4b': 100
    },
    'Superior Tribunal de Justiça': {
        '2ant': 100, '4a': 1000/9, '4b': 100, '6': 1000/7.5,
        '7a': 1000/7.5, '7b': 1000/7.5, '8': 1000/10, '10': 1000/10
    }
}
fatores_padrao_je = fatores_metas_por_ramo['Justiça Estadual']
ramos_definidos = set(fatores_metas_por_ramo.keys())


def calcular_meta(df: pd.DataFrame, col_j: str, col_d: str, col_s: str, fator: Optional[float]) -> str | float:
    try:
        if not all(c in df.columns for c in (col_j, col_d, col_s)): return 'NA'
        den = df[col_d].sum() - df[col_s].sum()
        if den == 0 or fator in [None, 'NA']: return 'NA'
        return round((df[col_j].sum() / den) * fator, 2)
    except Exception: return 'NA'


def gerar_grafico(df: pd.DataFrame, nome_meta: str, caminho_img: str):
    col_numerica = pd.to_numeric(df[nome_meta], errors='coerce')
    df_validos = df.copy()
    df_validos[nome_meta + '_val'] = col_numerica
    df_validos.dropna(subset=[nome_meta + '_val'], inplace=True)
    if df_validos.empty:
        log.warning(f"Nenhum valor válido para gerar gráfico de {nome_meta}.")
        return
    df_validos.sort_values(by=nome_meta + '_val', ascending=False, inplace=True)
    plt.figure(figsize=(max(16, len(df_validos) * 0.6), 10))
    plt.bar(df_validos['sigla_tribunal'], df_validos[nome_meta + '_val'], color='skyblue')
    plt.title(f'Comparação da {nome_meta.upper()} entre os Tribunais')
    plt.xticks(rotation=90, fontsize=8)
    plt.tight_layout()
    plt.savefig(caminho_img)
    plt.close()
    log.info(f"Gráfico salvo em: {caminho_img}")


def salvar_csv(df: pd.DataFrame, caminho: str):
    df.to_csv(caminho, index=False, encoding='utf-8', sep=';')
    log.info(f"Arquivo salvo: {caminho}")


def processar_metas(df: pd.DataFrame, fatores: dict) -> dict:
    metas = {}
    config_metas = {
        'meta2a':   ('julgm2_a', 'distm2_a', 'suspm2_a', fatores.get('2a')),
        'meta2b':   ('julgm2_b', 'distm2_b', 'suspm2_b', fatores.get('2b')),
        'meta2c':   ('julgm2_c', 'distm2_c', 'suspm2_c', fatores.get('2c')),
        'meta2ant': ('julgm2_ant', 'distm2_ant', 'suspm2_ant', fatores.get('2ant')),
        'meta4a':   ('julgm4_a', 'distm4_a', 'suspm4_a', fatores.get('4a')),
        'meta4b':   ('julgm4_b', 'distm4_b', 'suspm4_b', fatores.get('4b')),
        'meta6':    ('julgm6_a', 'distm6_a', 'suspm6_a', fatores.get('6')),
        'meta7a':   ('julgm7_a', 'distm7_a', 'suspm7_a', fatores.get('7a')),
        'meta7b':   ('julgm7_b', 'distm7_b', 'suspm7_b', fatores.get('7b')),
        'meta8a':   ('julgm8_a', 'distm8_a', 'suspm8_a', fatores.get('8a')),
        'meta8b':   ('julgm8_b', 'distm8_b', 'suspm8_b', fatores.get('8b')),
        'meta10a':  ('julgm10_a', 'distm10_a', 'suspm10_a', fatores.get('10a')),
        'meta10b':  ('julgm10_b', 'distm10_b', 'suspm10_b', fatores.get('10b')),
        'meta8_stj': ('julgm8', 'dism8', 'suspm8', fatores.get('8')),
        'meta10_stj': ('julgm10', 'dism10', 'suspm10', fatores.get('10')),
    }
    for meta, (j, d, s, f) in config_metas.items():
        metas[meta] = calcular_meta(df, j, d, s, f)
    return metas


t0 = time.perf_counter()
os.makedirs(PASTA_RESULTADOS, exist_ok=True)
log.info("Iniciando processamento de dados...")

arquivos_csv = [f for f in os.listdir(PASTA_CSV) if f.endswith('.csv')]
resultados, todos_dados = [], []

for arquivo in tqdm(arquivos_csv, desc="Lendo CSVs"):
    caminho = os.path.join(PASTA_CSV, arquivo)
    try:
        df = pd.read_csv(caminho)
        if df.empty or 'sigla_tribunal' not in df.columns: continue
        todos_dados.append(df)

        tribunal = df['sigla_tribunal'].iloc[0]
        ramo = df['ramo_justica'].iloc[0]
        if ramo == 'Tribunais Superiores':
            ramo = {'TST': 'Tribunal Superior do Trabalho', 'STJ': 'Superior Tribunal de Justiça'}.get(tribunal, ramo)
        elif ramo == 'Justiça Eleitoral':
            ramo = 'Tribunal Superior Eleitoral'

        fatores = fatores_metas_por_ramo.get(ramo, fatores_padrao_je)

        if all(c in df.columns for c in ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']):
            num = df['julgados_2025'].sum()
            den = df['casos_novos_2025'].sum() + df['dessobrestados_2025'].sum() - df['suspensos_2025'].sum()
            meta1 = round((num / den) * 100, 2) if den else 'NA'
        else:
            meta1 = 'NA'

        metas = processar_metas(df, fatores)
        linha = {'sigla_tribunal': tribunal, 'ramo_justica': ramo, 'meta1': meta1}
        linha.update(metas)
        resultados.append(linha)

    except Exception as e:
        log.error(f"Erro ao processar {arquivo}: {e}")

if todos_dados:
    log.info("Gerando arquivo consolidado...")
    salvar_csv(pd.concat(todos_dados, ignore_index=True), ARQUIVO_CONSOLIDADO)

if resultados:
    log.info("Gerando arquivo de resumo das metas...")
    df_final = pd.DataFrame(resultados)
    salvar_csv(df_final, ARQUIVO_RESUMO)
    log.info("Gerando gráfico comparativo da Meta 1...")
    gerar_grafico(df_final, 'meta1', GRAFICO_META1)

log.info(f"Tempo total de execução: {time.perf_counter() - t0:.2f} segundos")
