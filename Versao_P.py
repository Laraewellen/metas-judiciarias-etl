# Versao_P_Paralela.py - Corrigido com rich, tqdm preciso e remoção automática da pasta temporária
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
import concurrent.futures
from typing import Optional, Tuple
from tqdm import tqdm
from rich.logging import RichHandler
import logging
import shutil

# Logging configurado
logging.basicConfig(level="INFO", format="[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S", handlers=[RichHandler()])
log = logging.getLogger("rich")

# Diretórios e arquivos
PASTA_CSV = 'dados'
PASTA_RESULTADOS = 'resultados_versao_P'
PASTA_TEMP = os.path.join(PASTA_RESULTADOS, 'temp_para_consolidacao')
ARQUIVO_RESUMO = os.path.join(PASTA_RESULTADOS, 'ResumoMetas.csv')
ARQUIVO_CONSOLIDADO = os.path.join(PASTA_RESULTADOS, 'Consolidado.csv')
GRAFICO_META1 = os.path.join(PASTA_RESULTADOS, 'grafico_meta1_paralelo.png')

# Criação das pastas
os.makedirs(PASTA_RESULTADOS, exist_ok=True)
os.makedirs(PASTA_TEMP, exist_ok=True)

# Fatores por ramo (inalterados)
fatores_metas_por_ramo = {
    'Justiça Estadual': {
        '2a': 1000/8, '2b': 1000/9, '2c': 1000/9.5, '2ant': 100,
        '4a': 1000/6.5, '4b': 100, '6': 100,
        '7a': 1000/5, '7b': 1000/5, '8a': 1000/7.5, '8b': 1000/9,
        '10a': 1000/9, '10b': 1000/10
    },
    'Justiça do Trabalho': {'2a': 1000/9.4, '2ant': 100, '4a': 1000/7, '4b': 100},
    'Justiça Federal': {
        '2a': 1000/8.5, '2b': 100, '2ant': 100, '4a': 1000/7, '4b': 100,
        '6': 1000/3.5, '7a': 1000/3.5, '7b': 1000/3.5, '8a': 1000/7.5, '8b': 1000/9, '10a': 100
    },
    'Justiça Militar da União': {'2a': 1000/9.5, '2b': 1000/9.9, '2ant': 100, '4a': 1000/9.5, '4b': 1000/9.9},
    'Justiça Militar Estadual': {'2a': 1000/9, '2b': 1000/9.5, '2ant': 100, '4a': 1000/9.5, '4b': 1000/9.9},
    'Tribunal Superior Eleitoral': {'2a': 1000/7.0, '2b': 1000/9.9, '2ant': 100, '4a': 1000/9, '4b': 1000/5},
    'Tribunal Superior do Trabalho': {'2a': 1000/8.5, '2b': 1000/9.9, '2ant': 100, '4a': 1000/7, '4b': 100},
    'Superior Tribunal de Justiça': {
        '2ant': 100, '4a': 1000/9, '4b': 100, '6': 1000/7.5,
        '7a': 1000/7.5, '7b': 1000/7.5, '8': 1000/10, '10': 1000/10
    }
}
fatores_padrao_je = fatores_metas_por_ramo['Justiça Estadual']

def calcular_meta(df, col_j, col_d, col_s, fator):
    try:
        if not all(c in df.columns for c in (col_j, col_d, col_s)): return 'NA'
        den = df[col_d].sum() - df[col_s].sum()
        if den == 0 or fator in ['NA', None]: return 'NA'
        return round((df[col_j].sum() / den) * fator, 2)
    except: return 'NA'

def gerar_grafico(df: pd.DataFrame, nome_meta: str, caminho_img: str):
    df = df.copy()
    df[nome_meta + '_val'] = pd.to_numeric(df[nome_meta], errors='coerce')
    df.dropna(subset=[nome_meta + '_val'], inplace=True)
    if df.empty:
        log.warning(f"Sem dados válidos para gráfico de {nome_meta}.")
        return
    df.sort_values(by=nome_meta + '_val', ascending=False, inplace=True)
    plt.figure(figsize=(max(16, len(df) * 0.6), 10))
    plt.bar(df['sigla_tribunal'], df[nome_meta + '_val'], color='skyblue')
    plt.title(f'Comparação da {nome_meta.upper()} - Versão Paralela')
    plt.xticks(rotation=90, fontsize=8)
    plt.tight_layout()
    plt.savefig(caminho_img)
    plt.close()
    log.info(f"Gráfico salvo: {caminho_img}")

def processar_arquivo_temp(args: Tuple[str, int, int]) -> Optional[dict]:
    path, idx, total = args
    try:
        df = pd.read_csv(path)
        if df.empty or 'sigla_tribunal' not in df.columns: return None
        tribunal = df['sigla_tribunal'].iloc[0]
        ramo = df['ramo_justica'].iloc[0]
        if ramo == 'Tribunais Superiores':
            ramo = {'TST': 'Tribunal Superior do Trabalho', 'STJ': 'Superior Tribunal de Justiça'}.get(tribunal, ramo)
        elif ramo == 'Justiça Eleitoral':
            ramo = 'Tribunal Superior Eleitoral'
        fatores = fatores_metas_por_ramo.get(ramo, fatores_padrao_je)
        if all(c in df.columns for c in ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']):
            den = df['casos_novos_2025'].sum() + df['dessobrestados_2025'].sum() - df['suspensos_2025'].sum()
            meta1 = round((df['julgados_2025'].sum() / den) * 100, 2) if den else 'NA'
        else:
            meta1 = 'NA'
        config = {
            'meta2a': ('julgm2_a','distm2_a','suspm2_a','2a'),
            'meta2b': ('julgm2_b','distm2_b','suspm2_b','2b'),
            'meta2c': ('julgm2_c','distm2_c','suspm2_c','2c'),
            'meta2ant': ('julgm2_ant','distm2_ant','suspm2_ant','2ant'),
            'meta4a': ('julgm4_a','distm4_a','suspm4_a','4a'),
            'meta4b': ('julgm4_b','distm4_b','suspm4_b','4b'),
            'meta6': ('julgm6_a','distm6_a','suspm6_a','6'),
            'meta7a': ('julgm7_a','distm7_a','suspm7_a','7a'),
            'meta7b': ('julgm7_b','distm7_b','suspm7_b','7b'),
            'meta8a': ('julgm8_a','distm8_a','suspm8_a','8a'),
            'meta8b': ('julgm8_b','distm8_b','suspm8_b','8b'),
            'meta10a': ('julgm10_a','distm10_a','suspm10_a','10a'),
            'meta10b': ('julgm10_b','distm10_b','suspm10_b','10b'),
            'meta8_stj': ('julgm8','dism8','suspm8','8'),
            'meta10_stj': ('julgm10','dism10','suspm10','10')
        }
        metas = {'sigla_tribunal': tribunal, 'ramo_justica': ramo, 'meta1': meta1}
        for k, (j, d, s, fk) in config.items():
            fator = fatores.get(fk, fatores_padrao_je.get(fk, 'NA'))
            metas[k] = calcular_meta(df, j, d, s, fator)
        nome_temp = f"temp_{idx}.csv"
        df.to_csv(os.path.join(PASTA_TEMP, nome_temp), index=False, encoding='utf-8', sep=';')
        return metas
    except Exception:
        return None

if __name__ == '__main__':
    t0 = time.perf_counter()
    log.info("Iniciando processamento paralelo com arquivos temporários...")
    arquivos = [os.path.join(PASTA_CSV, f) for f in os.listdir(PASTA_CSV) if f.endswith('.csv')]
    tarefas = [(path, i, len(arquivos)) for i, path in enumerate(arquivos)]
    resultados = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(processar_arquivo_temp, tarefa) for tarefa in tarefas]
        for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Lendo CSVs"):
            resultado = f.result()
            if resultado: resultados.append(resultado)

    log.info("Gerando consolidado...")
    primeiro = True
    with open(ARQUIVO_CONSOLIDADO, 'wb') as out:
        for i in range(len(tarefas)):
            temp_path = os.path.join(PASTA_TEMP, f"temp_{i}.csv")
            if not os.path.exists(temp_path): continue
            with open(temp_path, 'rb') as f:
                if not primeiro: next(f)
                else: primeiro = False
                out.write(f.read())
    log.info(f"Arquivo salvo: {ARQUIVO_CONSOLIDADO}")

    try:
        shutil.rmtree(PASTA_TEMP)
        log.info(f"Pasta temporária removida: {PASTA_TEMP}")
    except Exception as e:
        log.warning(f"Falha ao remover pasta temporária: {e}")

    if resultados:
        log.info("Gerando resumo de metas...")
        df_resumo = pd.DataFrame(resultados)
        df_resumo.to_csv(ARQUIVO_RESUMO, index=False, encoding='utf-8', sep=';')
        log.info(f"Arquivo salvo: {ARQUIVO_RESUMO}")
        log.info("Gerando gráfico da Meta 1...")
        gerar_grafico(df_resumo, 'meta1', GRAFICO_META1)

    log.info(f"Tempo total de execução: {time.perf_counter() - t0:.2f} segundos")