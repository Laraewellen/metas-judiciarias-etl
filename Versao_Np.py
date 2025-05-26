import pandas as pd
import matplotlib.pyplot as plt
import os

print("-------------------------------------------")
print("Iniciando o processamento das Metas dos Tribunais...")
print("-------------------------------------------")

PASTA_CSV = 'dados'
PASTA_RESULTADOS = 'resultados_versao_NP'

print(f"\n[INFO] Verificando/Criando pasta de resultados: {PASTA_RESULTADOS}")
os.makedirs(PASTA_RESULTADOS, exist_ok=True)

ARQUIVO_RESUMO = os.path.join(PASTA_RESULTADOS, 'ResumoMetas.csv')
ARQUIVO_CONSOLIDADO = os.path.join(PASTA_RESULTADOS, 'Consolidado.csv')

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
ramos_definidos_com_fatores = set(fatores_metas_por_ramo.keys())

def calcular_meta(df, col_julgado, col_distribuido, col_suspenso, fator):
    try:
        colunas_necessarias = [col_julgado, col_distribuido, col_suspenso]
        if not all(coluna in df.columns for coluna in colunas_necessarias):
            return 'NA'
        num = df[col_julgado].sum()
        den = df[col_distribuido].sum() - df[col_suspenso].sum()
        if den == 0: return 'NA'
        if fator == 'NA' or fator is None: return 'NA'
        return round((num / den) * fator, 2)
    except Exception: return 'NA'

resultados = []
todos_dados = []

print("\n[INFO] Iniciando a leitura e processamento dos arquivos CSV da pasta 'dados'...")
arquivos_csv_existentes = [f for f in os.listdir(PASTA_CSV) if f.endswith('.csv')]
ramos_nao_mapeados_avisados = set()

if not arquivos_csv_existentes:
    print(f"[AVISO] Nenhum arquivo CSV encontrado na pasta: {PASTA_CSV}")
else:
    for i, arquivo in enumerate(arquivos_csv_existentes):
        print(f"  -> Lendo arquivo: {arquivo} ({i+1}/{len(arquivos_csv_existentes)})")
        caminho = os.path.join(PASTA_CSV, arquivo)
        try:
            df = pd.read_csv(caminho, sep=',', encoding='utf-8')
            todos_dados.append(df)

            if df.empty or 'sigla_tribunal' not in df.columns or 'ramo_justica' not in df.columns:
                print(f"    [AVISO] Arquivo {arquivo} está vazio ou não contém colunas essenciais. Pulando...")
                continue

            tribunal = df['sigla_tribunal'].iloc[0]
            ramo_justica_csv = df['ramo_justica'].iloc[0]
            ramo_para_fatores = ramo_justica_csv
            
            if ramo_justica_csv == "Tribunais Superiores":
                if tribunal == "TST": ramo_para_fatores = "Tribunal Superior do Trabalho"
                elif tribunal == "STJ": ramo_para_fatores = "Superior Tribunal de Justiça"
            elif ramo_justica_csv == "Justiça Eleitoral": ramo_para_fatores = "Tribunal Superior Eleitoral"

            if ramo_para_fatores not in ramos_definidos_com_fatores and ramo_justica_csv not in ramos_nao_mapeados_avisados:
                print(f"    [AVISO] Ramo de justiça '{ramo_justica_csv}' (Tribunal: {tribunal}, Mapeado para: '{ramo_para_fatores}') não possui um conjunto de fatores de metas específico. Serão utilizados fatores padrão (baseados na Justiça Estadual).")
                ramos_nao_mapeados_avisados.add(ramo_justica_csv)

            fatores_especificos_ramo = fatores_metas_por_ramo.get(ramo_para_fatores, {})
            meta1_calculada = 'NA'
            colunas_meta1 = ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']
            if all(coluna in df.columns for coluna in colunas_meta1):
                try:
                    num_meta1 = df['julgados_2025'].sum()
                    den_meta1 = df['casos_novos_2025'].sum() + df['dessobrestados_2025'].sum() - df['suspensos_2025'].sum()
                    if den_meta1 == 0: meta1_calculada = 'NA'
                    else: meta1_calculada = round((num_meta1 / den_meta1) * 100, 2)
                except Exception: meta1_calculada = 'NA'
            
            metas_calculadas = {'meta1': meta1_calculada}
            config_metas_gerais = {
                'meta2a':   {'cols': ('julgm2_a', 'distm2_a', 'suspm2_a'), 'f_key': '2a'},
                'meta2b':   {'cols': ('julgm2_b', 'distm2_b', 'suspm2_b'), 'f_key': '2b'},
                'meta2c':   {'cols': ('julgm2_c', 'distm2_c', 'suspm2_c'), 'f_key': '2c'},
                'meta2ant': {'cols': ('julgm2_ant', 'distm2_ant', 'suspm2_ant'), 'f_key': '2ant'},
                'meta4a':   {'cols': ('julgm4_a', 'distm4_a', 'suspm4_a'), 'f_key': '4a'},
                'meta4b':   {'cols': ('julgm4_b', 'distm4_b', 'suspm4_b'), 'f_key': '4b'},
                'meta6':    {'cols': ('julgm6_a', 'distm6_a', 'suspm6_a'), 'f_key': '6'},
                'meta7a':   {'cols': ('julgm7_a', 'distm7_a', 'suspm7_a'), 'f_key': '7a'},
                'meta7b':   {'cols': ('julgm7_b', 'distm7_b', 'suspm7_b'), 'f_key': '7b'},
                'meta8a':   {'cols': ('julgm8_a', 'distm8_a', 'suspm8_a'), 'f_key': '8a'},
                'meta8b':   {'cols': ('julgm8_b', 'distm8_b', 'suspm8_b'), 'f_key': '8b'},
                'meta10a':  {'cols': ('julgm10_a', 'distm10_a', 'suspm10_a'), 'f_key': '10a'},
                'meta10b':  {'cols': ('julgm10_b', 'distm10_b', 'suspm10_b'), 'f_key': '10b'},
            }

            for nome_meta, config in config_metas_gerais.items():
                fator_especifico = fatores_especificos_ramo.get(config['f_key'])
                fator_final = fator_especifico if fator_especifico is not None else fatores_padrao_je.get(config['f_key'], 'NA')
                col_j, col_d, col_s = config['cols']
                metas_calculadas[nome_meta] = calcular_meta(df, col_j, col_d, col_s, fator_final)

            if ramo_para_fatores == 'Superior Tribunal de Justiça':
                fator_stj_8 = fatores_especificos_ramo.get('8', 'NA')
                metas_calculadas['meta8_stj'] = calcular_meta(df, 'julgm8', 'dism8', 'suspm8', fator_stj_8)
                fator_stj_10 = fatores_especificos_ramo.get('10', 'NA')
                metas_calculadas['meta10_stj'] = calcular_meta(df, 'julgm10', 'dism10', 'suspm10', fator_stj_10)

            linha_resultado = {'sigla_tribunal': tribunal, 'ramo_justica': ramo_justica_csv}
            linha_resultado.update(metas_calculadas)
            resultados.append(linha_resultado)
        
        except pd.errors.EmptyDataError: print(f"    [ERRO] Arquivo {arquivo} está vazio ou mal formatado. Pulando...")
        except Exception as e: print(f"    [ERRO] Erro ao processar o arquivo {arquivo}: {e}. Pulando...")
    
    print("[INFO] Leitura e processamento dos arquivos CSV concluído.")

    if todos_dados:
        print("\n[INFO] Gerando arquivo consolidado...")
        df_consolidado = pd.concat(todos_dados, ignore_index=True)
        df_consolidado.to_csv(ARQUIVO_CONSOLIDADO, index=False, encoding='utf-8', sep=';')
        print(f"[OK] Arquivo consolidado salvo em: {ARQUIVO_CONSOLIDADO}")
    else: print("[AVISO] Nenhum dado para consolidar.")

    if resultados:
        print("\n[INFO] Gerando arquivo de resumo das metas...")
        df_resultados_final = pd.DataFrame(resultados)
        cols_principais = ['sigla_tribunal', 'ramo_justica', 'meta1']
        cols_metas_numeradas = sorted([col for col in df_resultados_final.columns if col.startswith('meta') and col != 'meta1' and not col.endswith('_stj')])
        cols_metas_stj = sorted([col for col in df_resultados_final.columns if col.endswith('_stj')])
        cols_outras = sorted([col for col in df_resultados_final.columns if col not in cols_principais and not col.startswith('meta')])
        
        colunas_finais_unicas = []
        for col_list in [cols_principais, cols_metas_numeradas, cols_metas_stj, cols_outras]:
            for col in col_list:
                if col in df_resultados_final.columns and col not in colunas_finais_unicas:
                    colunas_finais_unicas.append(col)
        for col in df_resultados_final.columns:
            if col not in colunas_finais_unicas: colunas_finais_unicas.append(col)
        
        df_resultados_final = df_resultados_final[colunas_finais_unicas]
        df_resultados_final.to_csv(ARQUIVO_RESUMO, index=False, encoding='utf-8', sep=';')
        print(f"[OK] Arquivo de resumo salvo em: {ARQUIVO_RESUMO}")

        if 'meta1' in df_resultados_final.columns:
            print("\n[INFO] Gerando gráfico da Meta 1...")
            df_resultados_final['meta1_numerica'] = pd.to_numeric(df_resultados_final['meta1'], errors='coerce')
            df_validos_grafico = df_resultados_final.dropna(subset=['meta1_numerica']).copy()
            
            if not df_validos_grafico.empty:
                df_validos_grafico.sort_values(by='meta1_numerica', ascending=False, inplace=True)
                num_tribunais = len(df_validos_grafico['sigla_tribunal'])
                fig_width = max(16, num_tribunais * 0.6)
                xtick_fontsize = max(6, min(10, 200 / num_tribunais if num_tribunais > 0 else 10))

                plt.figure(figsize=(fig_width, 10))
                plt.bar(df_validos_grafico['sigla_tribunal'], df_validos_grafico['meta1_numerica'], color='skyblue')
                plt.title('Comparação da Meta 1 entre os Tribunais (Valores Válidos Ordenados)', fontsize=16)
                plt.xlabel('Tribunal', fontsize=12)
                plt.ylabel('Meta 1 (%)', fontsize=12)
                plt.xticks(rotation=90, ha='center', fontsize=xtick_fontsize)
                plt.yticks(fontsize=10)
                plt.grid(axis='y', linestyle='--')
                plt.tight_layout()
                
                caminho_grafico = os.path.join(PASTA_RESULTADOS, 'grafico_meta1.png')
                plt.savefig(caminho_grafico)
                plt.close()
                print(f"[OK] Gráfico da Meta 1 salvo em: {caminho_grafico}")
            else: print("[AVISO] Não há dados válidos da Meta 1 para gerar o gráfico.")
        else: print("[AVISO] Coluna 'meta1' não encontrada no resumo para gerar o gráfico.")
    else: print("[AVISO] Nenhum resultado foi processado para gerar o resumo ou gráfico.")

print("\n-------------------------------------------")
print("[INFO] Processo finalizado!")
print(f"Verifique os arquivos na pasta: {PASTA_RESULTADOS}")
print("-------------------------------------------")