import pandas as pd
import matplotlib.pyplot as plt
import os
import concurrent.futures
import time

print("-------------------------------------------")
print("Iniciando o processamento PARALELO das Metas dos Tribunais (v3.1 - Consolidado Corrigido)")
print("-------------------------------------------")
time.sleep(0.5)

# Diretórios
PASTA_CSV = 'dados'
PASTA_RESULTADOS = 'resultados_versao_P'
PASTA_TEMP_CONSOLIDADO = os.path.join(PASTA_RESULTADOS, 'temp_para_consolidacao')

print(f"\n[INFO] Verificando/Criando pasta de resultados: {PASTA_RESULTADOS}")
os.makedirs(PASTA_RESULTADOS, exist_ok=True)

if os.path.exists(PASTA_TEMP_CONSOLIDADO):
    print(f"[INFO] Limpando pasta temporária de execuções anteriores: {PASTA_TEMP_CONSOLIDADO}")
    for f_temp in os.listdir(PASTA_TEMP_CONSOLIDADO):
        try:
            os.remove(os.path.join(PASTA_TEMP_CONSOLIDADO, f_temp))
        except Exception as e_remove:
            print(f"  [AVISO] Não foi possível remover o ficheiro temp {f_temp}: {e_remove}")
else:
    os.makedirs(PASTA_TEMP_CONSOLIDADO, exist_ok=True)
    print(f"[INFO] Criada pasta temporária: {PASTA_TEMP_CONSOLIDADO}")
time.sleep(0.2)

# Arquivos de saída
ARQUIVO_RESUMO = os.path.join(PASTA_RESULTADOS, 'ResumoMetas.csv')
ARQUIVO_CONSOLIDADO = os.path.join(PASTA_RESULTADOS, 'Consolidado.csv')
CAMINHO_GRAFICO = os.path.join(PASTA_RESULTADOS, 'grafico_meta1_paralelo.png')

fatores_metas_por_ramo = {
    'Justiça Estadual': {
        '2a': 1000/8, '2b': 1000/9, '2c': 1000/9.5, '2ant': 100, '4a': 1000/6.5, '4b': 100, '6': 100,
        '7a': 1000/5, '7b': 1000/5, '8a': 1000/7.5, '8b': 1000/9, '10a': 1000/9, '10b': 1000/10
    },
    'Justiça do Trabalho': { '2a': 1000/9.4, '2ant': 100, '4a': 1000/7, '4b': 100 },
    'Justiça Federal': {
        '2a': 1000/8.5, '2b': 100, '2ant': 100, '4a': 1000/7, '4b': 100, '6': 1000/3.5,
        '7a': 1000/3.5, '7b': 1000/3.5, '8a': 1000/7.5, '8b': 1000/9, '10a': 100
    },
    'Justiça Militar da União': { '2a': 1000/9.5, '2b': 1000/9.9, '2ant': 100, '4a': 1000/9.5, '4b': 1000/9.9 },
    'Justiça Militar Estadual': { '2a': 1000/9, '2b': 1000/9.5, '2ant': 100, '4a': 1000/9.5, '4b': 1000/9.9 },
    'Tribunal Superior Eleitoral': { '2a': 1000/7.0, '2b': 1000/9.9, '2ant': 100, '4a': 1000/9, '4b': 1000/5 },
    'Tribunal Superior do Trabalho': { '2a': 1000/8.5, '2b': 1000/9.9, '2ant': 100, '4a': 1000/7, '4b': 100 },
    'Superior Tribunal de Justiça': {
        '2ant': 100, '4a': 1000/9, '4b': 100, '6': 1000/7.5, '7a': 1000/7.5, '7b': 1000/7.5,
        '8': 1000/10, '10': 1000/10
    }
}
fatores_padrao_je = fatores_metas_por_ramo['Justiça Estadual']
ramos_definidos_com_fatores = set(fatores_metas_por_ramo.keys())

def calcular_meta(df, col_julgado, col_distribuido, col_suspenso, fator):
    try:
        if not all(col in df.columns for col in [col_julgado, col_distribuido, col_suspenso]):
            return 'NA'
        num = df[col_julgado].sum()
        den = df[col_distribuido].sum() - df[col_suspenso].sum()
        if den == 0 or fator in ['NA', None]: return 'NA'
        return round((num / den) * fator, 2)
    except: return 'NA'

def processar_arquivo_csv(tarefa_info):
    caminho_arquivo_completo, indice_atual, total_arquivos, pasta_temp_para_worker = tarefa_info
    nome_arquivo = os.path.basename(caminho_arquivo_completo)
    print(f"  -> Lendo e processando arquivo: {nome_arquivo} ({indice_atual}/{total_arquivos})...")
    
    caminho_csv_temporario_final = None
    try:
        df = pd.read_csv(caminho_arquivo_completo, sep=',', encoding='utf-8')
        if df.empty:
            return None, None, f"[AVISO WORKER] Arquivo {nome_arquivo} ({indice_atual}/{total_arquivos}) está vazio."

        pid = os.getpid()
        nome_arquivo_saneado = "".join(c if c.isalnum() or c in ('.', '_', '-') else '_' for c in nome_arquivo)
        nome_temp_csv = f"temp_consolidar_{pid}_{indice_atual}_{nome_arquivo_saneado}.csv"
        caminho_csv_temporario_final = os.path.join(pasta_temp_para_worker, nome_temp_csv)
        df.to_csv(caminho_csv_temporario_final, index=False, encoding='utf-8', sep=';')

        if 'sigla_tribunal' not in df.columns or 'ramo_justica' not in df.columns:
             return None, caminho_csv_temporario_final, f"[AVISO WORKER] Arquivo {nome_arquivo} não tem sigla_tribunal ou ramo_justica para cálculo de metas, mas os dados brutos foram salvos temporariamente."

        tribunal = df['sigla_tribunal'].iloc[0]
        ramo_justica_csv = df['ramo_justica'].iloc[0] # Usar o nome da coluna original do CSV
        ramo_para_fatores = ramo_justica_csv # Nome da variável que será usada para buscar os fatores
        aviso_ramo_str = None 
        if ramo_justica_csv == "Tribunais Superiores":
            if tribunal == "TST": ramo_para_fatores = "Tribunal Superior do Trabalho"
            elif tribunal == "STJ": ramo_para_fatores = "Superior Tribunal de Justiça"
        elif ramo_justica_csv == "Justiça Eleitoral": ramo_para_fatores = "Tribunal Superior Eleitoral"

        if ramo_para_fatores not in ramos_definidos_com_fatores:
            aviso_ramo_str = f"[AVISO] Ramo '{ramo_justica_csv}' (Tribunal: {tribunal}, Arquivo: {nome_arquivo}, Mapeado para: '{ramo_para_fatores}') sem fatores específicos. Usando padrão JE."
            
        fatores_do_ramo_atual = fatores_metas_por_ramo.get(ramo_para_fatores, {})
        meta1_valor = 'NA' # Nome da variável alterado para evitar confusão
        if all(col in df.columns for col in ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']):
            try:
                num = df['julgados_2025'].sum()
                den = df['casos_novos_2025'].sum() + df['dessobrestados_2025'].sum() - df['suspensos_2025'].sum()
                if den != 0: meta1_valor = round((num / den) * 100, 2)
            except: meta1_valor = 'NA'
        
        metas = {'meta1': meta1_valor}
        configuracoes = {
            'meta2a': ('julgm2_a', 'distm2_a', 'suspm2_a', '2a'), 'meta2b': ('julgm2_b', 'distm2_b', 'suspm2_b', '2b'),
            'meta2c': ('julgm2_c', 'distm2_c', 'suspm2_c', '2c'), 'meta2ant': ('julgm2_ant', 'distm2_ant', 'suspm2_ant', '2ant'),
            'meta4a': ('julgm4_a', 'distm4_a', 'suspm4_a', '4a'), 'meta4b': ('julgm4_b', 'distm4_b', 'suspm4_b', '4b'),
            'meta6':  ('julgm6_a', 'distm6_a', 'suspm6_a', '6'),  'meta7a': ('julgm7_a', 'distm7_a', 'suspm7_a', '7a'),
            'meta7b': ('julgm7_b', 'distm7_b', 'suspm7_b', '7b'), 'meta8a': ('julgm8_a', 'distm8_a', 'suspm8_a', '8a'),
            'meta8b': ('julgm8_b', 'distm8_b', 'suspm8_b', '8b'), 'meta10a':('julgm10_a', 'distm10_a', 'suspm10_a', '10a'),
            'meta10b':('julgm10_b', 'distm10_b', 'suspm10_b', '10b'),
        }
        for nome_meta, (cj, cd, cs, fk) in configuracoes.items():
            fator = fatores_do_ramo_atual.get(fk, fatores_padrao_je.get(fk, 'NA'))
            metas[nome_meta] = calcular_meta(df, cj, cd, cs, fator)
        if ramo_para_fatores == "Superior Tribunal de Justiça":
            metas['meta8_stj'] = calcular_meta(df, 'julgm8', 'dism8', 'suspm8', fatores_do_ramo_atual.get('8', 'NA'))
            metas['meta10_stj'] = calcular_meta(df, 'julgm10', 'dism10', 'suspm10', fatores_do_ramo_atual.get('10', 'NA'))
        
        linha_resultado = {'sigla_tribunal': tribunal, 'ramo_justica': ramo_justica_csv} # Usar o ramo original do CSV aqui
        linha_resultado.update(metas)
        return linha_resultado, caminho_csv_temporario_final, aviso_ramo_str
        
    except Exception as e:
        return None, None, f"[ERRO WORKER CRÍTICO] Ao processar {nome_arquivo} ({indice_atual}/{total_arquivos}): {e}"

if __name__ == '__main__':
    print("\n[INFO] Iniciando a preparação para o processamento PARALELO dos arquivos CSV...")
    
    nomes_arquivos_csv = [f for f in os.listdir(PASTA_CSV) if f.endswith('.csv')]
    total_de_arquivos = len(nomes_arquivos_csv)
    tarefas_para_processar = []
    if nomes_arquivos_csv:
        for i, nome_f in enumerate(nomes_arquivos_csv):
            caminho_completo = os.path.join(PASTA_CSV, nome_f)
            tarefas_para_processar.append((caminho_completo, i + 1, total_de_arquivos, PASTA_TEMP_CONSOLIDADO))
    
    resultados_finais_resumo = []
    paths_arquivos_temporarios_coletados = [] 
    avisos_gerais_unicos = set()

    if not tarefas_para_processar:
        print(f"[AVISO] Nenhum arquivo CSV encontrado em {PASTA_CSV} para processar.")
    else:
        print(f"[INFO] {total_de_arquivos} arquivos CSV serão processados em paralelo.")
        
        with concurrent.futures.ProcessPoolExecutor() as executor:
            future_results = executor.map(processar_arquivo_csv, tarefas_para_processar)
            print("\n[INFO] Aguardando e recolhendo resultados dos processos...")

            for resultado_tuplo in future_results:
                if resultado_tuplo is not None:
                    linha_res, path_temp_csv, aviso_retornado = resultado_tuplo
                    if linha_res: resultados_finais_resumo.append(linha_res)
                    if path_temp_csv: paths_arquivos_temporarios_coletados.append(path_temp_csv)
                    if aviso_retornado: avisos_gerais_unicos.add(aviso_retornado)
    
    print("[INFO] Processamento paralelo dos arquivos individuais concluído.")

    if avisos_gerais_unicos:
        print("\n--- Avisos Gerados Durante o Processamento ---")
        for av in sorted(list(avisos_gerais_unicos)): print(av)
        print("---------------------------------------------")

    # --- SECÇÃO CORRIGIDA PARA GERAR O ARQUIVO CONSOLIDADO ---
    if paths_arquivos_temporarios_coletados:
        print(f"\n[INFO] Iniciando geração do arquivo consolidado a partir de {len(paths_arquivos_temporarios_coletados)} arquivos temporários...")
        
        primeiro_arquivo_consolidado = True
        # Abre o ficheiro consolidado final em modo de escrita de TEXTO (não binário aqui, pois vamos lidar com linhas de texto)
        with open(ARQUIVO_CONSOLIDADO, 'w', encoding='utf-8', newline='') as f_out_consolidado:
            for i, temp_path in enumerate(paths_arquivos_temporarios_coletados):
                if temp_path is None or not os.path.exists(temp_path):
                    print(f"  [AVISO] Arquivo temporário não encontrado ou inválido: {temp_path}. Pulando.")
                    continue
                
                # print(f"  -> Juntando dados do arquivo temporário: {os.path.basename(temp_path)} ({i+1}/{len(paths_arquivos_temporarios_coletados)})")
                try:
                    with open(temp_path, 'r', encoding='utf-8') as f_in_temp:
                        if primeiro_arquivo_consolidado:
                            # Copia todo o conteúdo do primeiro ficheiro (incluindo cabeçalho)
                            for line in f_in_temp: # Lê linha a linha para preservar newlines corretamente
                                f_out_consolidado.write(line)
                            primeiro_arquivo_consolidado = False
                        else:
                            # Para os ficheiros seguintes, pula o cabeçalho e copia o resto
                            next(f_in_temp) # Pula a primeira linha (cabeçalho)
                            for line in f_in_temp: # Copia as linhas restantes
                                f_out_consolidado.write(line)
                    os.remove(temp_path) # Remove o ficheiro temporário após o uso
                except Exception as e:
                    print(f"    [ERRO] ao processar/juntar o arquivo temporário {temp_path}: {e}")
        
        if not primeiro_arquivo_consolidado: 
            print(f"[OK] Arquivo consolidado salvo em: {ARQUIVO_CONSOLIDADO}")
        else:
            print(f"[AVISO] Nenhum arquivo temporário foi processado com sucesso para consolidação.")
            if os.path.exists(ARQUIVO_CONSOLIDADO) and os.path.getsize(ARQUIVO_CONSOLIDADO) == 0:
                 print(f"[INFO] O arquivo {ARQUIVO_CONSOLIDADO} está vazio e será removido.")
                 try: os.remove(ARQUIVO_CONSOLIDADO)
                 except: pass
    else:
        print("[AVISO] Nenhum arquivo temporário foi gerado para consolidação.")
    
    try:
        if os.path.exists(PASTA_TEMP_CONSOLIDADO) and not os.listdir(PASTA_TEMP_CONSOLIDADO):
            os.rmdir(PASTA_TEMP_CONSOLIDADO)
            print(f"[INFO] Pasta temporária {PASTA_TEMP_CONSOLIDADO} removida.")
    except OSError as e:
        print(f"[AVISO] Não foi possível remover a pasta temporária {PASTA_TEMP_CONSOLIDADO}: {e}")

    # --- O RESTO DO SCRIPT (ResumoMetas.csv e Gráfico) ---
    if resultados_finais_resumo:
        print("\n[INFO] Gerando arquivo de resumo das metas...")
        df_resultados_final = pd.DataFrame(resultados_finais_resumo)
        cols_principais = ['sigla_tribunal', 'ramo_justica', 'meta1']
        cols_metas_numeradas = sorted([col for col in df_resultados_final.columns if col.startswith('meta') and col != 'meta1' and not col.endswith('_stj')])
        cols_metas_stj = sorted([col for col in df_resultados_final.columns if col.endswith('_stj')])
        cols_outras = sorted([col for col in df_resultados_final.columns if col not in cols_principais and not col.startswith('meta')])
        colunas_finais_unicas = []
        for col_list in [cols_principais, cols_metas_numeradas, cols_metas_stj, cols_outras]:
            for col in col_list:
                if col in df_resultados_final.columns and col not in colunas_finais_unicas: colunas_finais_unicas.append(col)
        for col in df_resultados_final.columns:
            if col not in colunas_finais_unicas: colunas_finais_unicas.append(col)
        
        df_resultados_final = df_resultados_final[colunas_finais_unicas]
        df_resultados_final.to_csv(ARQUIVO_RESUMO, index=False, encoding='utf-8', sep=';')
        print(f"[OK] Arquivo de resumo salvo em: {ARQUIVO_RESUMO}")

        if 'meta1' in df_resultados_final.columns:
            print("\n[INFO] Gerando gráfico da Meta 1...")
            df_para_grafico = df_resultados_final.copy()
            df_para_grafico['meta1_numerica'] = pd.to_numeric(df_para_grafico['meta1'], errors='coerce')
            df_validos_grafico = df_para_grafico.dropna(subset=['meta1_numerica'])
            
            if not df_validos_grafico.empty:
                df_validos_grafico = df_validos_grafico.sort_values(by='meta1_numerica', ascending=False)
                num_tribunais = len(df_validos_grafico['sigla_tribunal'])
                fig_width = max(16, num_tribunais * 0.6); xtick_fontsize = max(6, min(10, 200 / num_tribunais if num_tribunais > 0 else 10))
                plt.figure(figsize=(fig_width, 10))
                plt.bar(df_validos_grafico['sigla_tribunal'], df_validos_grafico['meta1_numerica'], color='skyblue')
                plt.title('Comparação da Meta 1 (Versão Paralela Otimizada)', fontsize=16)
                plt.xlabel('Tribunal', fontsize=12); plt.ylabel('Meta 1 (%)', fontsize=12)
                plt.xticks(rotation=90, ha='center', fontsize=xtick_fontsize); plt.yticks(fontsize=10)
                plt.grid(axis='y', linestyle='--'); plt.tight_layout()
                plt.savefig(CAMINHO_GRAFICO); plt.close()
                print(f"[OK] Gráfico da Meta 1 salvo em: {CAMINHO_GRAFICO}")
            else: print("[AVISO] Não há dados válidos da Meta 1 para gerar o gráfico.")
        else: print("[AVISO] Coluna 'meta1' não encontrada no resumo para gerar o gráfico.")
    else: print("[AVISO] Nenhum resultado foi processado para gerar o resumo ou gráfico.")

    print("\n-------------------------------------------")
    print("[INFO] Processo PARALELO (otimizado) finalizado!")
    print(f"Verifique os arquivos na pasta: {PASTA_RESULTADOS}")
    print("-------------------------------------------")