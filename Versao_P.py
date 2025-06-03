import os
import time
import pandas as pd
import matplotlib.pyplot as plt
import concurrent.futures
from typing import Optional, Tuple, Dict
from tqdm import tqdm
from rich.logging import RichHandler
import logging
import shutil
from datetime import datetime
import csv

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
PASTA_RESULTADOS = 'resultados_versao_P'
PASTA_TEMP = os.path.join(PASTA_RESULTADOS, 'temp_para_consolidacao')
ARQUIVO_RESUMO = os.path.join(PASTA_RESULTADOS, 'ResumoMetas.csv')
ARQUIVO_CONSOLIDADO = os.path.join(PASTA_RESULTADOS, 'Consolidado.csv')
GRAFICO_META1 = os.path.join(PASTA_RESULTADOS, 'grafico_meta1.png')

fatores_metas_por_ramo = {
    'Justiça Estadual': {'2a': 1000/8, '2b': 1000/9, '2c': 1000/9.5, '2ant': 100,
                          '4a': 1000/6.5, '4b': 100, '6': 100, '7a': 1000/5, '7b': 1000/5,
                          '8a': 1000/7.5, '8b': 1000/9, '10a': 1000/9, '10b': 1000/10},
    'Justiça do Trabalho': {'2a': 1000/9.4, '2ant': 100, '4a': 1000/7, '4b': 100},
    'Justiça Federal': {'2a': 1000/8.5, '2b': 100, '2ant': 100, '4a': 1000/7, '4b': 100,
                         '6': 1000/3.5, '7a': 1000/3.5, '7b': 1000/3.5, '8a': 1000/7.5,
                         '8b': 1000/9, '10a': 100},
    'Justiça Militar da União': {'2a': 1000/9.5, '2b': 1000/9.9, '2ant': 100,
                                  '4a': 1000/9.5, '4b': 1000/9.9},
    'Justiça Militar Estadual': {'2a': 1000/9, '2b': 1000/9.5, '2ant': 100,
                                  '4a': 1000/9.5, '4b': 1000/9.9},
    'Tribunal Superior Eleitoral': {'2a': 1000/7.0, '2b': 1000/9.9, '2ant': 100,
                                     '4a': 1000/9, '4b': 1000/5},
    'Tribunal Superior do Trabalho': {'2a': 1000/8.5, '2b': 1000/9.9, '2ant': 100,
                                       '4a': 1000/7, '4b': 100},
    'Superior Tribunal de Justiça': {'2ant': 100, '4a': 1000/9, '4b': 100,
                                      '6': 1000/7.5, '7a': 1000/7.5, '7b': 1000/7.5,
                                      '8': 1000/10, '10': 1000/10}
}
fatores_padrao_je = fatores_metas_por_ramo['Justiça Estadual']

def calcular_meta_geral(df: pd.DataFrame, col_j: str, col_d: str, col_s: str, fator: Optional[float]) -> str | float:
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
    df_plot = df.copy()
    df_plot[nome_meta + '_val'] = pd.to_numeric(df_plot[nome_meta], errors='coerce')
    df_plot = df_plot.dropna(subset=[nome_meta + '_val'])
    if df_plot.empty:
        log.warning(f"Sem dados válidos para o gráfico de {nome_meta}")
        return

    df_plot = df_plot.sort_values(by=nome_meta + '_val', ascending=False)
    plt.figure(figsize=(max(16, len(df_plot) * 0.6), 10))
    plt.bar(df_plot['sigla_tribunal'], df_plot[nome_meta + '_val'], color='steelblue')
    plt.title(f'Comparativo {nome_meta.upper()}')
    plt.xticks(rotation=90, fontsize=8)
    plt.tight_layout()
    plt.savefig(caminho_img)
    plt.close()
    log.info(f"Gráfico salvo em {caminho_img}")

def processar_arquivo_individual(args: Tuple[str, int, int]) -> Optional[Tuple[Dict, str, Optional[str]]]:
    caminho_do_arquivo, idx, total_arquivos = args
    nome_do_arquivo = os.path.basename(caminho_do_arquivo)
    aviso_processamento = None
    metas_calculadas: Dict[str, str | float] = {} 
    
    try:
        df = pd.read_csv(caminho_do_arquivo, sep=',', encoding='utf-8', on_bad_lines='skip')

        if df.empty:
            return None, None, f"Arquivo {nome_do_arquivo} ({idx}/{total_arquivos}) vazio."

        pid_processo = os.getpid()
        timestamp_atual = datetime.now().strftime('%Y%m%d_%H%M%S')
        nome_arquivo_temporario = f"temp_{timestamp_atual}_{pid_processo}_{idx}_{nome_do_arquivo.replace(' ', '_').replace('.csv', '')}.csv"
        caminho_arquivo_temporario = os.path.join(PASTA_TEMP, nome_arquivo_temporario)
        
        df.to_csv(caminho_arquivo_temporario, 
                  index=False, 
                  encoding='utf-8', 
                  sep=';', 
                  quoting=csv.QUOTE_NONNUMERIC)

        if 'sigla_tribunal' not in df.columns or 'ramo_justica' not in df.columns:
            return None, caminho_arquivo_temporario, f"Arquivo {nome_do_arquivo} sem coluna 'sigla_tribunal' ou 'ramo_justica'"

        tribunal_atual = df['sigla_tribunal'].iloc[0]
        ramo_justica_atual = df['ramo_justica'].iloc[0]
        
        fatores_do_ramo, ramo_mapeado = obter_fatores_por_ramo(ramo_justica_atual, tribunal_atual)
        fatores_je_padronizados = fatores_metas_por_ramo['Justiça Estadual']

        meta1_final = 'NA'
        colunas_meta1_obrigatorias = ['julgados_2025', 'casos_novos_2025', 'suspensos_2025']
        if all(c in df.columns and df[c].notna().any() for c in colunas_meta1_obrigatorias):
            s_julgados_m1 = df['julgados_2025'].sum()
            s_casos_novos_m1 = df['casos_novos_2025'].sum()
            s_suspensos_m1 = df['suspensos_2025'].sum()
            
            s_dessobrestados_m1 = 0
            if 'dessobrestados_2025' in df.columns and df['dessobrestados_2025'].notna().any():
                s_dessobrestados_m1 = df['dessobrestados_2025'].sum()

            if pd.isna(s_julgados_m1):
                 meta1_final = 'NA'
            else:
                den_m1 = s_casos_novos_m1 + s_dessobrestados_m1 - s_suspensos_m1
                if den_m1 == 0:
                    meta1_final = 'NA'
                else:
                    meta1_final = round((s_julgados_m1 / den_m1) * 100, 2)
        metas_calculadas['meta1'] = meta1_final

        configuracoes_outras_metas = {
            'meta2a': ('julgm2_a', 'distm2_a', 'suspm2_a', '2a'),
            'meta2b': ('julgm2_b', 'distm2_b', 'suspm2_b', '2b'),
            'meta2c': ('julgm2_c', 'distm2_c', 'suspm2_c', '2c'),
            'meta2ant': ('julgm2_ant', 'distm2_ant', 'suspm2_ant', '2ant'),
            'meta4a': ('julgm4_a', 'distm4_a', 'suspm4_a', '4a'),
            'meta4b': ('julgm4_b', 'distm4_b', 'suspm4_b', '4b'),
            'meta6': ('julgm6_a', 'distm6_a', 'suspm6_a', '6'),
            'meta7a': ('julgm7_a', 'distm7_a', 'suspm7_a', '7a'),
            'meta7b': ('julgm7_b', 'distm7_b', 'suspm7_b', '7b'),
            'meta8a': ('julgm8_a', 'distm8_a', 'suspm8_a', '8a'),
            'meta8b': ('julgm8_b', 'distm8_b', 'suspm8_b', '8b'),
            'meta10a': ('julgm10_a', 'distm10_a', 'suspm10_a', '10a'),
            'meta10b': ('julgm10_b', 'distm10_b', 'suspm10_b', '10b'),
        }

        for nome_meta_chave, (j_col, d_col, s_col, chave_fator) in configuracoes_outras_metas.items():
            fator_aplicar = fatores_do_ramo.get(chave_fator, fatores_je_padronizados.get(chave_fator, 'NA'))
            metas_calculadas[nome_meta_chave] = calcular_meta_geral(df, j_col, d_col, s_col, fator_aplicar)

        if ramo_mapeado == "Superior Tribunal de Justiça":
            if '8' in fatores_do_ramo:
                metas_calculadas['meta8_stj'] = calcular_meta_geral(df, 'julgm8', 'dism8', 'suspm8', fatores_do_ramo.get('8'))
                if metas_calculadas.get('meta8_stj') != 'NA':
                    metas_calculadas.pop('meta8a', None)
                    metas_calculadas.pop('meta8b', None)
            if '10' in fatores_do_ramo:
                metas_calculadas['meta10_stj'] = calcular_meta_geral(df, 'julgm10', 'dism10', 'suspm10', fatores_do_ramo.get('10'))
                if metas_calculadas.get('meta10_stj') != 'NA':
                    metas_calculadas.pop('meta10a', None)
                    metas_calculadas.pop('meta10b', None)
        
        linha_resultado_final = {'sigla_tribunal': tribunal_atual, 'ramo_justica': ramo_justica_atual}
        linha_resultado_final.update(metas_calculadas)

        return linha_resultado_final, caminho_arquivo_temporario, aviso_processamento

    except Exception as e_process:
        log.error(f"[ERRO] Falha no arquivo {nome_do_arquivo}: {e_process}", exc_info=True)
        return None, None, f"Erro crítico no arquivo {nome_do_arquivo}"


if __name__ == '__main__':
    t0_exec = time.perf_counter()
    log.info("==== INICIANDO PROCESSAMENTO PARALELO ====")

    if not os.path.exists(PASTA_CSV):
        log.error(f"Pasta de dados '{PASTA_CSV}' não encontrada.")
        exit(1)

    os.makedirs(PASTA_RESULTADOS, exist_ok=True)
    os.makedirs(PASTA_TEMP, exist_ok=True)

    arquivos_csv_para_processar = [f for f in os.listdir(PASTA_CSV) if f.endswith('.csv')]
    num_total_csv = len(arquivos_csv_para_processar)

    lista_tarefas = [(os.path.join(PASTA_CSV, nome_arq), i + 1, num_total_csv) for i, nome_arq in enumerate(arquivos_csv_para_processar)]

    resultados_finais = []
    arquivos_temporarios_gerados = []
    avisos_gerais = set()

    if not lista_tarefas:
        log.warning("Nenhum CSV encontrado para processar.")
    else:
        log.info(f"Serão processados {num_total_csv} arquivos.")
        num_workers = max(1, os.cpu_count() - 1 if os.cpu_count() else 1)
        log.info(f"Utilizando {num_workers} workers.")
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
            map_results = list(tqdm(
                executor.map(processar_arquivo_individual, lista_tarefas),
                total=num_total_csv, desc="Lendo CSVs (Paralelo)"))

            for resultado_map in map_results:
                if resultado_map:
                    linha_res, path_tmp, aviso_res = resultado_map
                    if linha_res: resultados_finais.append(linha_res)
                    if path_tmp: arquivos_temporarios_gerados.append(path_tmp)
                    if aviso_res: avisos_gerais.add(aviso_res)
    
    if arquivos_temporarios_gerados:
        log.info(f"Concatenando {len(arquivos_temporarios_gerados)} arquivos para o consolidado...")
        with open(ARQUIVO_CONSOLIDADO, 'wb') as f_out_consolidado:
            primeiro_arquivo = True
            for arquivo_temp_path in tqdm(arquivos_temporarios_gerados, desc="Concatenando"):
                try:
                    with open(arquivo_temp_path, 'rb') as f_in_temp:
                        if primeiro_arquivo:
                            f_out_consolidado.write(f_in_temp.read())
                            primeiro_arquivo = False
                        else:
                            next(f_in_temp) 
                            for linha_bytes in f_in_temp:
                                f_out_consolidado.write(linha_bytes)
                    os.remove(arquivo_temp_path)
                except Exception as e_concat:
                    log.error(f"Erro ao concatenar ou remover arquivo temporário {arquivo_temp_path}: {e_concat}")
        log.info(f"Consolidado salvo: {ARQUIVO_CONSOLIDADO}")
    else:
        log.warning("Nenhum arquivo temporário para consolidar.")

    if resultados_finais:
        df_resumo_agregado = pd.DataFrame(resultados_finais)
        df_resumo_agregado = df_resumo_agregado.astype(str).replace('nan', 'NA')
        
        cols_principais = ['sigla_tribunal', 'ramo_justica', 'meta1']
        cols_metas_num = sorted([c for c in df_resumo_agregado.columns if c.startswith('meta') and c != 'meta1' and not c.endswith('_stj')])
        cols_metas_stj_list = sorted([c for c in df_resumo_agregado.columns if c.endswith('_stj')])
        
        ordem_colunas_resumo = cols_principais + cols_metas_num + cols_metas_stj_list
        outras_cols_resumo = sorted([c for c in df_resumo_agregado.columns if c not in ordem_colunas_resumo])
        ordem_colunas_resumo.extend(outras_cols_resumo)
        
        df_resumo_agregado = df_resumo_agregado[ordem_colunas_resumo]
        df_resumo_agregado.to_csv(ARQUIVO_RESUMO, index=False, encoding='utf-8', sep=';')
        log.info(f"Resumo salvo em: {ARQUIVO_RESUMO}")
        
        if 'meta1' in df_resumo_agregado.columns:
            gerar_grafico(df_resumo_agregado, 'meta1', GRAFICO_META1)
    else:
        log.warning("Nenhum resultado para gerar resumo.")

    if avisos_gerais:
        log.warning("===== AVISOS DO PROCESSAMENTO PARALELO =====")
        for aviso_item in avisos_gerais:
            log.warning(aviso_item)
        log.warning("==========================================")

    try:
        if os.path.exists(PASTA_TEMP):
            shutil.rmtree(PASTA_TEMP)
            log.info(f"Pasta temporária {PASTA_TEMP} removida.")
    except Exception as e_rm_temp:
        log.warning(f"Falha ao remover pasta temporária {PASTA_TEMP}: {e_rm_temp}")

    tempo_total_exec = time.perf_counter() - t0_exec
    log.info(f"Processamento paralelo concluído em {tempo_total_exec:.2f} segundos.")