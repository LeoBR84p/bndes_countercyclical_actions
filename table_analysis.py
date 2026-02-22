# %% SCRIPT DE GERAÇÃO DE TABELAS DE ANÁLISE
# Importando as bibliotecas necessárias

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr
import json
from pathlib import Path
import pyarrow.parquet as pq
from paths import INPUTS_PATH, OUTPUTS_PATH, RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, REGRESSION_TABLES_PATH, REGRESSION_MODELS_PATH, REGRESSION_TESTS_PATH

# Configurando o estilo do seaborn para as tabelas e gráficos
sns.set_theme(
    style="white",
    context="paper",
    palette="colorblind",
    font="serif",
    font_scale=1.0,
    rc={
        "figure.figsize": (12, 6),
        "axes.titlesize": 11,
        "axes.labelsize": 10,
        "axes.titleweight": "bold",
        "lines.linewidth": 1.2,
        "lines.markersize": 4,
        "legend.fontsize": 9,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "axes.spines.top": False,
        "axes.spines.right": False
    }
)
# %% TABELA 1 - ESTATÍSTICAS DESCRITIVAS
# TABELA 1 - ESTATÍSTICAS DESCRITIVAS

# Carregando os dados processados(
df1 = pq.read_table(Path(FINAL_DATA_PATH) / 'painel1c.parquet').to_pandas()
df2 = pq.read_table(Path(FINAL_DATA_PATH) / 'painel2c.parquet').to_pandas()
df3 = pq.read_table(Path(FINAL_DATA_PATH) / 'painel3c.parquet').to_pandas()
df4 = pq.read_table(Path(FINAL_DATA_PATH) / 'painel4c.parquet').to_pandas()

# Selecionando as colunas de interesse
# Selecionar apenas colunas necessárias
df1_sel = df1[['codigo', 'estado', 'ano', 'delta_log_pib_real', 'share_desembolso_real_pib_real_ano_anterior', 'populacao']]
df2_sel = df2[['codigo', 'estado', 'ano', 'delta_asinh_va_industria_real', 'share_desembolso_industria_real_ano_anterior']]
df3_sel = df3[['codigo', 'estado', 'ano', 'delta_asinh_va_agropecuaria_real', 'share_desembolso_agropecuaria_real_ano_anterior']]
df4_sel = df4[['codigo', 'estado', 'ano', 'delta_log_pibpc_real']]

df_desc = (
    df1_sel
    .merge(df2_sel, on=['codigo','estado','ano'], how='inner')
    .merge(df3_sel, on=['codigo','estado','ano'], how='inner')
    .merge(df4_sel, on=['codigo','estado','ano'], how='inner')
)

# ordenar colunas
colunas_ordenadas = ['codigo', 'estado', 'ano', 'delta_log_pib_real', 'delta_log_pibpc_real', 'populacao', 'share_desembolso_real_pib_real_ano_anterior', 'delta_asinh_va_industria_real', 'share_desembolso_industria_real_ano_anterior', 'delta_asinh_va_agropecuaria_real', 'share_desembolso_agropecuaria_real_ano_anterior' ]
colunas_ordenadas_existentes = [col for col in colunas_ordenadas if col in df_desc.columns]
df_desc = df_desc[colunas_ordenadas_existentes]
df_desc = df_desc.rename(columns={
    'delta_log_pib_real': 'Δ log PIB',
    'delta_log_pibpc_real': 'Δ log PIBpc',
    'delta_asinh_va_industria_real': 'Δ asinh VA Indústria',
    'delta_asinh_va_agropecuaria_real': 'Δ asinh VA Agro',
    'share_desembolso_real_pib_real_ano_anterior': 'Desembolso/PIB(t-1)',
    'share_desembolso_industria_real_ano_anterior': 'Desembolso Indústria/PIB(t-1)',
    'share_desembolso_agropecuaria_real_ano_anterior': 'Desembolso Agro/PIB(t-1)',
    'populacao': 'População'
})

# Gerar tabela de estatísticas descritivas, com exceção de ano, código e estado
tabela1 = df_desc.drop(columns=['codigo', 'estado', 'ano']).describe(percentiles=[0.25, 0.5, 0.75]).T
tabela1 = tabela1[['count','mean','std','25%','50%','75%']]
tabela1.columns = ['N','Média','DP','P25','Mediana','P75']
tabela1 = tabela1.round(3)
print(tabela1)

# Notas: A amostra compreende 5.570 municípios brasileiros ao longo do período analisado (painel município–ano). As variáveis dependentes são expressas em variação anual logarítmica (Δ log) ou transformação asinh, conforme indicado. As variáveis de desembolso correspondem ao valor no ano t dividido pelo PIB (ou valor adicionado setorial) no ano t−1. Valores monetários estão expressos em termos reais com ano base 2021. Estatísticas reportam número de observações não nulas por variável.
# %% TABELA 2 - CORRELAÇÕES ENTRE VARIÁVEIS
# TABELA 2 - CORRELAÇÕES ENTRE VARIÁVEIS

# Matriz de correlação (Pearson)
tabela2 = df_desc.drop(columns=['codigo', 'estado', 'ano']).corr(method='pearson')

# Calcular matriz de p-valores
cols = tabela2.columns
pvals = pd.DataFrame(index=cols, columns=cols)

for i in cols:
    for j in cols:
        r, p = pearsonr(df_desc[i], df_desc[j])
        pvals.loc[i, j] = p # type: ignore

pvals = pvals.astype(float)

# Máscara triângulo superior
mask = np.triu(np.ones_like(tabela2, dtype=bool))
corr_lower = tabela2.mask(mask).round(3)

plt.figure(figsize=(14, 8))
sns.heatmap(
    corr_lower,
    mask=mask,
    cmap="coolwarm",
    vmin=-1, vmax=1,
    center=0,
    annot=True,
    fmt=".2f",
    square=True,
    cbar_kws={"shrink": 0.8}
)
plt.title("Matriz de Correlação (Triângulo Inferior)")
plt.tight_layout()
plt.show()

# Notas: Correlações de Pearson calculadas a partir da amostra município–ano. Dado o tamanho da amostra, praticamente todas as correlações são estatisticamente significativas ao nível convencional.

# %% TABELA 3 - PANINEL A ATÈ D
# TABELA 3 - PAINEL A - Δ log PIB

# %% TABELA 3 - PAINÉIS A ATÉ D
# TABELA 3 - Resultados (2-way FE município+ano; Leads 1-2; Lags 1-3; Cluster UF+Ano)

def stars(p: float) -> str:
    if p < 0.01:
        return "***"
    if p < 0.05:
        return "**"
    if p < 0.10:
        return "*"
    return ""

def mapear_horizonte(var: str) -> str | None:
    # ordem desejada: Lead 2, Lead 1, Lag 1, Lag 2, Lag 3
    if "_lead2" in var:
        return "Lead 2"
    if "_lead1" in var:
        return "Lead 1"
    if "_lag1" in var:
        return "Lag 1"
    if "_lag2" in var:
        return "Lag 2"
    if "_lag3" in var:
        return "Lag 3"
    return None

def montar_painel(coef_path: Path, wald_path: Path, share_base: str, painel_nome: str) -> tuple[pd.DataFrame, dict]:
    """
    Retorna:
      - DataFrame com linhas (Lead2, Lead1, Lag1, Lag2, Lag3) e colunas (Coef, EP)
      - dicionário com p-valores relevantes (wald_leads, wald_acumulado, wald_betas se houver)
    """
    coef_df = pq.read_table(coef_path).to_pandas()
    wald_df = pq.read_table(wald_path).to_pandas()

    # Filtrar apenas a família do share desejado (e seus leads/lags)
    df = coef_df[coef_df["var"].astype(str).str.startswith(share_base)].copy()

    # Mapear horizontes e manter apenas leads/lags requeridos
    df["h"] = df["var"].astype(str).apply(mapear_horizonte)
    df = df.dropna(subset=["h"])

    ordem = ["Lead 2", "Lead 1", "Lag 1", "Lag 2", "Lag 3"]
    df["h"] = pd.Categorical(df["h"], categories=ordem, ordered=True)
    df = df.sort_values("h")

    # Formatar coef e SE
    df["Coef"] = df.apply(lambda x: f"{x['coef']:.3f}{stars(float(x['p']))}", axis=1)
    df["EP"] = df["std_err"].apply(lambda x: f"({float(x):.3f})")

    painel = df.set_index("h")[["Coef", "EP"]]

    # Garantir todas as linhas na ordem, mesmo que falte alguma
    painel = painel.reindex(ordem)

    # Extrair testes
    info = {}
    for test_name in ["wald_leads", "wald_acumulado", "wald_betas"]:
        sub = wald_df[wald_df["test"] == test_name]
        if len(sub) > 0:
            info[test_name] = {
                "stat": float(sub["stat"].iloc[0]),
                "pval": float(sub["pval"].iloc[0]),
                "df": int(sub["df"].iloc[0]),
            }
    return painel, info


# === Definir arquivos e share_base por painel
painel_specs = [
    # Painel A: PIB
    {
        "painel": "Painel A - Δ log PIB",
        "coef_file": "model1c_pib_complementar_uf_cluster_coef.parquet",
        "wald_file": "model1c_pib_complementar_uf_cluster_wald_tests.parquet",
        "share_base": "share_desembolso_real_pib_real_ano_anterior",
    },
    # Painel B: Indústria
    {
        "painel": "Painel B - Δ asinh VA Indústria",
        "coef_file": "model2c_va_industria_complementar_uf_cluster_coef.parquet",
        "wald_file": "model2c_va_industria_complementar_uf_cluster_wald_tests.parquet",
        "share_base": "share_desembolso_industria_real_ano_anterior",
    },
    # Painel C: Agro
    {
        "painel": "Painel C - Δ asinh VA Agropecuária",
        "coef_file": "model3c_va_agropecuaria_complementar_uf_cluster_coef.parquet",
        "wald_file": "model3c_va_agropecuaria_complementar_uf_cluster_wald_tests.parquet",
        "share_base": "share_desembolso_agropecuaria_real_ano_anterior",
    },
    # Painel D: PIBpc
    {
        "painel": "Painel D - Δ log PIBpc",
        "coef_file": "model4c_pibpc_complementar_uf_cluster_coef.parquet",
        "wald_file": "model4c_pibpc_complementar_uf_cluster_wald_tests.parquet",
        "share_base": "share_desembolso_real_pib_real_ano_anterior",
    },
]

# === Montar painéis ===
paineis = []
notas_testes = {}

for spec in painel_specs:
    coef_path = Path(REGRESSION_TABLES_PATH) / spec["coef_file"]
    wald_path = Path(REGRESSION_TESTS_PATH) / spec["wald_file"]

    painel_df, info = montar_painel(
        coef_path=coef_path,
        wald_path=wald_path,
        share_base=spec["share_base"],
        painel_nome=spec["painel"],
    )

    # adicionar multiindex (painel, horizonte)
    painel_df = painel_df.copy()
    painel_df["Painel"] = spec["painel"]
    painel_df = painel_df.reset_index().rename(columns={"h": "Horizonte"})
    painel_df = painel_df.set_index(["Painel", "Horizonte"])

    paineis.append(painel_df)
    notas_testes[spec["painel"]] = info

tabela3 = pd.concat(paineis).sort_index()

print("\nTABELA 3 (Painéis A–D):")
print(tabela3)

print("\nNotas (p-valores de testes conjuntos por painel):")
for painel, info in notas_testes.items():
    p_leads = info.get("wald_leads", {}).get("pval", None)
    p_acum = info.get("wald_acumulado", {}).get("pval", None)
    print(f"- {painel}: p(wald_leads)={p_leads:.4f}" if p_leads is not None else f"- {painel}: p(wald_leads)=NA")
    print(f"           p(wald_acumulado)={p_acum:.4f}" if p_acum is not None else f"           p(wald_acumulado)=NA")
# Notas: Todas as regressões incluem efeitos fixos de município e ano (2-way FE). Erros-padrão com cluster duplo por Unidade da Federação e ano. A variável explicativa corresponde ao share de desembolso no ano t dividido pelo PIB (ou valor adicionado setorial) no ano t−1. Leads (t+1, t+2) testam ausência de pré-tendência. Lags (t−1 a t−3) capturam o efeito dinâmico. Asteriscos indicam níveis de significância: *** p<0,01; ** p<0,05; * p<0,10.
# %%# - EVENT STUDY - GRÁFICOS DE COEFICIENTES POR LAG/LEAD COM IC 95%
# 
# FIGURA 1 ~ img/grafico5.svg
# FIGURA 2 ~ img/grafico7.svg
# FIGURA 3 ~ img/grafico9.svg
# FIGURA 4 ~ img/grafico11.svg

# %% TABELA 4 - ROBUSTEZ CONSOLIDADA (Wald leads e acumulado)
# TABELA 4 - ROBUSTEZ CONSOLIDADA (Wald leads e acumulado)

df_wald_A = pq.read_table(Path(REGRESSION_TESTS_PATH) / "model1c_pib_complementar_uf_cluster_wald_tests.parquet").to_pandas()
df_wald_B = pq.read_table(Path(REGRESSION_TESTS_PATH) / "model2c_va_industria_complementar_uf_cluster_wald_tests.parquet").to_pandas()
df_wald_C = pq.read_table(Path(REGRESSION_TESTS_PATH) / "model3c_va_agropecuaria_complementar_uf_cluster_wald_tests.parquet").to_pandas()
df_wald_D = pq.read_table(Path(REGRESSION_TESTS_PATH) / "model4c_pibpc_complementar_uf_cluster_wald_tests.parquet").to_pandas()

# extrair p-valores
pA_leads = df_wald_A.loc[df_wald_A["test"]=="wald_leads","pval"].iloc[0] if (df_wald_A["test"]=="wald_leads").any() else np.nan
pA_acum  = df_wald_A.loc[df_wald_A["test"]=="wald_acumulado","pval"].iloc[0] if (df_wald_A["test"]=="wald_acumulado").any() else np.nan
pA_betas = df_wald_A.loc[df_wald_A["test"]=="wald_betas", "pval"].iloc[0] if (df_wald_A["test"]=="wald_betas").any() else np.nan

pB_leads = df_wald_B.loc[df_wald_B["test"]=="wald_leads","pval"].iloc[0] if (df_wald_B["test"]=="wald_leads").any() else np.nan
pB_acum  = df_wald_B.loc[df_wald_B["test"]=="wald_acumulado","pval"].iloc[0] if (df_wald_B["test"]=="wald_acumulado").any() else np.nan
pB_betas = df_wald_B.loc[df_wald_B["test"]=="wald_betas", "pval"].iloc[0] if (df_wald_B["test"]=="wald_betas").any() else np.nan

pC_leads = df_wald_C.loc[df_wald_C["test"]=="wald_leads","pval"].iloc[0] if (df_wald_C["test"]=="wald_leads").any() else np.nan
pC_acum  = df_wald_C.loc[df_wald_C["test"]=="wald_acumulado","pval"].iloc[0] if (df_wald_C["test"]=="wald_acumulado").any() else np.nan
pC_betas = df_wald_C.loc[df_wald_C["test"]=="wald_betas", "pval"].iloc[0] if (df_wald_C["test"]=="wald_betas").any() else np.nan

pD_leads = df_wald_D.loc[df_wald_D["test"]=="wald_leads","pval"].iloc[0] if (df_wald_D["test"]=="wald_leads").any() else np.nan
pD_acum  = df_wald_D.loc[df_wald_D["test"]=="wald_acumulado","pval"].iloc[0] if (df_wald_D["test"]=="wald_acumulado").any() else np.nan
pD_betas = df_wald_D.loc[df_wald_D["test"]=="wald_betas", "pval"].iloc[0] if (df_wald_D["test"]=="wald_betas").any() else np.nan

tabela4 = pd.DataFrame({
    "Resultado": ["Δ log PIB", "Δ asinh VA Indústria", "Δ asinh VA Agro", "Δ log PIBpc"],
    "p(wald_betas)": [pA_betas, pB_betas, pC_betas, pD_betas],
    "p(wald_leads)": [pA_leads, pB_leads, pC_leads, pD_leads],
    "p(wald_acumulado)": [pA_acum, pB_acum, pC_acum, pD_acum],
})

tabela4[["p(wald_betas)", "p(wald_leads)", "p(wald_acumulado)"]] = tabela4[["p(wald_betas)", "p(wald_leads)", "p(wald_acumulado)"]].round(4)

print(tabela4)
# %%
