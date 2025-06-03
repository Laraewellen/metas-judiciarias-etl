import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional
from tqdm import tqdm
from rich.logging import RichHandler
import logging

def obter_fatores_por_ramo(ramo_justica: str, sigla_tribunal: str) -> tuple[dict, str]:
    mapeamento_especial = {
        'Tribunais Superiores': {
            'TST': 'Tribunal Superior do Trabalho',
            'STJ': 'Superior Tribunal de Justiça'
        },
        'Justiça Eleitoral': 'Tribunal Superior Eleitoral'
    }

    if ramo_justica == 'Tribunais Superiores':
        ramo_usado = mapeamento_especial[ramo_justica].get(sigla_tribunal, ramo_justica)
    elif ramo_justica == 'Justiça Eleitoral':
        ramo_usado = mapeamento_especial[ramo_justica]
    else:
        ramo_usado = ramo_justica

    if ramo_usado in fatores_metas_por_ramo:
        return fatores_metas_por_ramo[ramo_usado], ramo_usado
    else:
        log.warning(f"[AVISO] Ramo '{ramo_justica}' (tribunal: {sigla_tribunal}) não tem fator específico. Usando padrão JE.")
        return fatores_padrao_je, 'Justiça Estadual'

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
    'Justiça do Trabalho': { '2a': 1000/9.4, '2ant': 100, '4a': 1000/7, '4b': 100 },
    'Justiça Federal': {
        '2a': 1000/8.5, '2b': 100, '2ant': 100, '4a': 1000/7, '4b': 100,
        '6': 1000/3.5, '7a': 1000/3.5, '7b': 1000/3.5, '8a': 1000/7.5, '8b': 1000/9, '10a': 100
    },
    'Justiça Militar da União': { '2a': 1000/9.5, '2b': 1000/9.9, '2ant': 100, '4a': 1000/9.5, '4b': 1000/9.9 },
    'Justiça Militar Estadual': { '2a': 1000/9, '2b': 1000/9.5, '2ant': 100, '4a': 1000/9.5, '4b': 1000/9.9 },
    'Tribunal Superior Eleitoral': { '2a': 1000/7.0, '2b': 1000/9.9, '2ant': 100, '4a': 1000/9, '4b': 1000/5 },
    'Tribunal Superior do Trabalho': { '2a': 1000/8.5, '2b': 1000/9.9, '2ant': 100, '4a': 1000/7, '4b': 100 },
    'Superior Tribunal de Justiça': {
        '2ant': 100, '4a': 1000/9, '4b': 100, '6': 1000/7.5,
        '7a': 1000/7.5, '7b': 1000/7.5, '8': 1000/10, '10': 1000/10
    }
}
fatores_padrao_je = fatores_metas_por_ramo['Justiça Estadual']
ramos_definidos_com_fatores = set(fatores_metas_por_ramo.keys()) 

def calcular_meta(df: pd.DataFrame, col_j: str, col_d: str, col_s: str, fator: Optional[float]) -> str | float:
    try:
        if not all(col in df.columns and df[col].notna().any() for col in (col_j, col_d, col_s)):
            return 'NA'
        
        numerador = df[col_j].sum()
        if pd.isna(numerador):
            return 'NA'
            
        den = df[col_d].sum() - df[col_s].sum()
        
        if den == 0 or fator == 'NA' or pd.isna(fator):
            return 'NA'
            
        return round((numerador / den) * fator, 2)
    except Exception:
        return 'NA'

def gerar_grafico(df: pd.DataFrame, nome_meta: str, caminho_img: str):
    df_para_grafico = df.copy()
    df_para_grafico[nome_meta + '_val'] = pd.to_numeric(df_para_grafico[nome_meta], errors='coerce')
    df_validos = df_para_grafico.dropna(subset=[nome_meta + '_val'])
    if df_validos.empty:
        log.warning(f"Nenhum valor válido para gerar gráfico de {nome_meta}.")
        return
    df_validos = df_validos.sort_values(by=nome_meta + '_val', ascending=False)
    plt.figure(figsize=(max(16, len(df_validos) * 0.6), 10))
    plt.bar(df_validos['sigla_tribunal'], df_validos[nome_meta + '_val'], color='skyblue')
    plt.title(f'Comparação da {nome_meta.upper()} entre os Tribunais (NP)')
    plt.xticks(rotation=90, fontsize=8, ha='center')
    plt.tight_layout()
    plt.savefig(caminho_img)
    plt.close()
    log.info(f"Gráfico salvo em: {caminho_img}")

def salvar_csv(df: pd.DataFrame, caminho: str):
    df.to_csv(caminho, index=False, encoding='utf-8', sep=';')
    log.info(f"Arquivo salvo: {caminho}")

def processar_outras_metas(df: pd.DataFrame, fatores_ramo_atual: dict, fatores_je: dict) -> dict:
    metas_calculadas = {}
    config_metas = {
        'meta2a':   ('julgm2_a', 'distm2_a', 'suspm2_a', '2a'),
        'meta2b':   ('julgm2_b', 'distm2_b', 'suspm2_b', '2b'),
        'meta2c':   ('julgm2_c', 'distm2_c', 'suspm2_c', '2c'),
        'meta2ant': ('julgm2_ant', 'distm2_ant', 'suspm2_ant', '2ant'),
        'meta4a':   ('julgm4_a', 'distm4_a', 'suspm4_a', '4a'),
        'meta4b':   ('julgm4_b', 'distm4_b', 'suspm4_b', '4b'),
        'meta6':    ('julgm6_a', 'distm6_a', 'suspm6_a', '6'),
        'meta7a':   ('julgm7_a', 'distm7_a', 'suspm7_a', '7a'),
        'meta7b':   ('julgm7_b', 'distm7_b', 'suspm7_b', '7b'),
        'meta8a':   ('julgm8_a', 'distm8_a', 'suspm8_a', '8a'),
        'meta8b':   ('julgm8_b', 'distm8_b', 'suspm8_b', '8b'),
        'meta10a':  ('julgm10_a', 'distm10_a', 'suspm10_a', '10a'),
        'meta10b':  ('julgm10_b', 'distm10_b', 'suspm10_b', '10b'),
    }
    for meta_nome, (j, d, s, f_key) in config_metas.items():
        fator = fatores_ramo_atual.get(f_key, fatores_je.get(f_key, 'NA'))
        metas_calculadas[meta_nome] = calcular_meta(df, j, d, s, fator)
    
    if '8' in fatores_ramo_atual: 
        metas_calculadas['meta8_stj'] = calcular_meta(df, 'julgm8', 'dism8', 'suspm8', fatores_ramo_atual.get('8'))
        if metas_calculadas.get('meta8_stj') != 'NA':
            metas_calculadas.pop('meta8a', None)
            metas_calculadas.pop('meta8b', None)
            
    if '10' in fatores_ramo_atual:
        metas_calculadas['meta10_stj'] = calcular_meta(df, 'julgm10', 'dism10', 'suspm10', fatores_ramo_atual.get('10'))
        if metas_calculadas.get('meta10_stj') != 'NA':
            metas_calculadas.pop('meta10a', None)
            metas_calculadas.pop('meta10b', None)
            
    return metas_calculadas

t0 = time.perf_counter()
os.makedirs(PASTA_RESULTADOS, exist_ok=True)
log.info("Iniciando processamento de dados (Versão NP)...")

arquivos_csv = [f for f in os.listdir(PASTA_CSV) if f.endswith('.csv')]
resultados, todos_dados = [], []
ramos_nao_mapeados_avisados = set() 

NOME_ARQUIVO_DEBUG = "TRF5 - Seção Judiciária do Ceará.csv" 

if not arquivos_csv:
    log.warning(f"Nenhum arquivo CSV encontrado em {PASTA_CSV}.")
else:
    for arquivo in tqdm(arquivos_csv, desc="Lendo CSVs (NP)"):
        caminho_completo = os.path.join(PASTA_CSV, arquivo)
        try:
            df = pd.read_csv(caminho_completo, sep=',', encoding='utf-8', on_bad_lines='skip')
            
            if df.empty or 'sigla_tribunal' not in df.columns or 'ramo_justica' not in df.columns:
                log.warning(f"Arquivo {arquivo} está vazio ou não contém colunas essenciais. Pulando...")
                continue
            
            todos_dados.append(df)

            tribunal_sigla = df['sigla_tribunal'].iloc[0]
            ramo_justica_original = df['ramo_justica'].iloc[0] 
            
            fatores_atuais, ramo_mapeado_para_fatores = obter_fatores_por_ramo(ramo_justica_original, tribunal_sigla)

            if ramo_mapeado_para_fatores not in ramos_definidos_com_fatores and ramo_justica_original not in ramos_nao_mapeados_avisados:
                ramos_nao_mapeados_avisados.add(ramo_justica_original)
           
            meta1_calculada = 'NA'
            colunas_meta1_base = ['julgados_2025', 'casos_novos_2025', 'suspensos_2025']
            
            debug_mode = (arquivo == NOME_ARQUIVO_DEBUG)
            if debug_mode:
                log.info(f"\n--- [DEBUG NP] INICIANDO DEBUG PARA: {arquivo} ---")
            
            if all(c in df.columns and df[c].notna().any() for c in colunas_meta1_base):
                s_julgados = df['julgados_2025'].sum()
                s_casos_novos = df['casos_novos_2025'].sum()
                s_suspensos = df['suspensos_2025'].sum()
                
                s_dessobrestados = 0
                if 'dessobrestados_2025' in df.columns and df['dessobrestados_2025'].notna().any():
                    s_dessobrestados = df['dessobrestados_2025'].sum()

                if pd.isna(s_julgados):
                    meta1_calculada = 'NA'
                    if debug_mode: log.info(f"[DEBUG NP] {arquivo} - Meta 1: NA (soma julgados_2025 é NaN)")
                else:
                    denominador_meta1 = s_casos_novos + s_dessobrestados - s_suspensos
                    if debug_mode:
                        log.info(f"[DEBUG NP] {arquivo} - Numerador (soma julgados_2025): {s_julgados}")
                        log.info(f"[DEBUG NP] {arquivo} - Denom. Componentes: CN={s_casos_novos}, DS={s_dessobrestados}, SP={s_suspensos}")
                        log.info(f"[DEBUG NP] {arquivo} - Denominador Final Meta 1: {denominador_meta1}")

                    if denominador_meta1 == 0:
                        meta1_calculada = 'NA'
                        if debug_mode: log.info(f"[DEBUG NP] {arquivo} - Meta 1: NA (denominador zero)")
                    else:
                        meta1_bruta = (s_julgados / denominador_meta1) * 100
                        meta1_calculada = round(meta1_bruta, 2)
                        if debug_mode:
                            log.info(f"[DEBUG NP] {arquivo} - Meta 1 (sem arredondar): {meta1_bruta}")
                            log.info(f"[DEBUG NP] {arquivo} - Meta 1 (COM arredondar): {meta1_calculada}")
            else:
                if debug_mode: log.warning(f"[DEBUG NP] {arquivo} - Colunas base para Meta 1 não encontradas ou sem dados válidos.")
                meta1_calculada = 'NA'

            if debug_mode:
                log.info(f"--- [DEBUG NP] FIM DEBUG PARA: {arquivo} ---\n")

            outras_metas_calculadas = processar_outras_metas(df, fatores_atuais, fatores_padrao_je)
            
            linha_res = {'sigla_tribunal': tribunal_sigla, 'ramo_justica': ramo_justica_original, 'meta1': meta1_calculada}
            linha_res.update(outras_metas_calculadas)
            resultados.append(linha_res)

        except pd.errors.EmptyDataError:
            log.error(f"Arquivo {arquivo} está vazio ou mal formatado. Pulando...")
        except Exception as e:
            log.error(f"Erro geral ao processar o arquivo {arquivo}: {e}", exc_info=True)

if todos_dados:
    log.info("Gerando arquivo consolidado...")
    df_consolidado = pd.concat(todos_dados, ignore_index=True) 
    salvar_csv(df_consolidado, ARQUIVO_CONSOLIDADO)

if resultados:
    log.info("Gerando arquivo de resumo das metas...")
    df_resumo_final = pd.DataFrame(resultados)
    df_resumo_final = df_resumo_final.astype(str).replace('nan', 'NA')
    
    cols_principais_ordenadas = ['sigla_tribunal', 'ramo_justica', 'meta1']
    cols_metas_numeradas_ordenadas = sorted([col for col in df_resumo_final.columns if col.startswith('meta') and col != 'meta1' and not col.endswith('_stj')])
    cols_metas_stj_ordenadas = sorted([col for col in df_resumo_final.columns if col.endswith('_stj')])
    
    colunas_ordenadas_final = cols_principais_ordenadas + cols_metas_numeradas_ordenadas + cols_metas_stj_ordenadas
    outras_colunas_existentes = sorted([col for col in df_resumo_final.columns if col not in colunas_ordenadas_final])
    colunas_ordenadas_final.extend(outras_colunas_existentes)
            
    df_resumo_final = df_resumo_final[colunas_ordenadas_final]
    salvar_csv(df_resumo_final, ARQUIVO_RESUMO)
    
    if 'meta1' in df_resumo_final.columns:
        log.info("Gerando gráfico comparativo da Meta 1...")
        gerar_grafico(df_resumo_final, 'meta1', GRAFICO_META1)

log.info(f"Tempo total de execução: {time.perf_counter() - t0:.2f} segundos")
print("-------------------------------------------")
print("[INFO] Processo NP finalizado!")
print(f"Verifique os arquivos na pasta: {PASTA_RESULTADOS}")
print("-------------------------------------------")