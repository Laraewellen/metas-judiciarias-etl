# 🧾 TP06 – Análise de Metas do Judiciário com Processamento Paralelo

### 👩‍💻 Desenvolvido por: Lara Ewellen De Carvalho Rocha  
**Disciplina**: Programação Concorrente e Paralela – 1º Semestre de 2025

---

## 📌 Objetivo

Este projeto visa processar dados estatísticos dos tribunais brasileiros, extraídos de arquivos CSV, e calcular o desempenho de cada um nas **metas nacionais do Judiciário**. O código foi implementado em duas versões:

- 🧱 **Versão Não Paralela** (`Versao_Np.py`)
- ⚡ **Versão Paralela** (`Versao_P.py`), utilizando `ProcessPoolExecutor`

Ambas as versões realizam:
- Leitura e validação de arquivos CSV
- Cálculo das metas 1, 2, 4, 6, 7, 8 e 10 (e variações)
- Geração de arquivos `ResumoMetas.csv` e `Consolidado.csv`
- Criação de um gráfico de barras da **Meta 1**

---

## 🗃️ Estrutura de Pastas Esperada

├── dados/ # Arquivos CSV de entrada

├── resultados_versao_NP/ # Resultados da versão não paralela

├── resultados_versao_P/ # Resultados da versão paralela

├── Versao_Np.py

├── Versao_P.py

└── README.md

---

## ⚙️ Como Executar

> É necessário ter Python 3.10+ com as bibliotecas: `pandas`, `matplotlib`, `tqdm`, `rich`.

1. Instale as dependências (se necessário):

pip install pandas matplotlib tqdm rich


2. Clone este repositório: [Repositório Metas Judiciárias ETL](https://github.com/Laraewellen/metas-judiciarias-etl.git)

3. Execute a versão **não paralela**:
   
5. Execute a versão **paralela**:


---

## 📊 Arquivos Gerados

| Arquivo                             | Descrição                                         |
|-------------------------------------|--------------------------------------------------|
| `ResumoMetas.csv`                   | Resultados agregados por tribunal                |
| `Consolidado.csv`                   | Todos os dados CSV unidos                        |
| `grafico_meta1.png`                 | Gráfico de barras comparando os tribunais        |

---

## 🚀 Comparativo de Desempenho

Foram realizados testes em **4 computadores diferentes** com múltiplas execuções. O relatório completo contendo os **tempos, speedups e gráficos** está no arquivo:

📄 `relatorio_comparativo_completo_speedup_4pcs.pdf`

---

## 🧠 Observações Técnicas

- A paralelização foi feita a nível de **processo**, utilizando `concurrent.futures.ProcessPoolExecutor`.
- Cada arquivo CSV é processado independentemente, garantindo **isolamento e escalabilidade**.
- O sistema é tolerante a erros de formatação, arquivos vazios e colunas ausentes.

---





