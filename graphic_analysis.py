# %% GRÁFICOS PARA ANÁLISE
# Importando as bibliotecas necessárias para a análise gráfica
import re
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import pyarrow.parquet as pq
from paths import OUTPUTS_PATH, REGRESSION_MODELS_PATH, IMAGES_PATH, REGRESSION_TABLES_PATH, REGRESSION_TESTS_PATH, RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH

# Configurando o estilo dos gráficos
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

#%% GRÁFICO 1
# Gráfico com a proporção dos Desembolsos do BNDES vis a vis PIB Real
# Carregando os dados
df_pib_hab = pq.read_table(Path(FINAL_DATA_PATH) / 'painel1.parquet').to_pandas()
df_bndes_total = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_total.parquet').to_pandas()

# Agrupando os dados por ano
df_pib_hab = df_pib_hab.groupby('ano')['pib_corrente'].sum().reset_index()
df_bndes_total = df_bndes_total.groupby('ano')['desembolsos_corrente'].sum().reset_index()

# Garantir compatibilidade de tipos para merge
df_pib_hab['ano'] = df_pib_hab['ano'].astype(str)
df_bndes_total['ano'] = df_bndes_total['ano'].astype(str)

# Unir os dados
df_merged = pd.merge(df_pib_hab, df_bndes_total, left_on='ano', right_on='ano', how='inner')

# Calcular proporção BNDES/PIB em percentual
df_merged['BNDES_pct_PIB'] = (df_merged['desembolsos_corrente'] / df_merged['pib_corrente']) * 100

# Converter ano para inteiro para facilitar visualização
df_merged['ano'] = pd.to_numeric(df_merged['ano'], errors='coerce').astype('int64')

# Criar o gráfico de proporção BNDES/PIB
plt.plot(df_merged['ano'], df_merged['BNDES_pct_PIB'], 
         marker='o', linewidth=2.5, markersize=7,
         color='blue', label='BNDES/PIB')

# Preencher área abaixo da linha
plt.fill_between(df_merged['ano'], df_merged['BNDES_pct_PIB'], 
                 alpha=0.1, color='gray')

# Linha de média
media = df_merged['BNDES_pct_PIB'].mean()
plt.axhline(y=media, color='red', linestyle=':', linewidth=2,
            label=f'Média: {media:.2f}%')

# Destacar período da crise financeira global
plt.axvspan(2008, 2010, alpha=0.15, color='orange', label='Crise Financeira Global')

# Configurações do gráfico
plt.xlabel('Ano')
plt.xticks(df_merged['ano']) # Ajustar valores de ano para inteiro sem casas decimais no gráfico
plt.ylabel('Desembolsos BNDES (% do PIB)')
plt.title('Proporção dos Desembolsos do BNDES em relação ao PIB')
plt.legend(loc='upper left')
plt.tight_layout()

# Salvar o gráfico como SVG para alta qualidade e escalabilidade
plt.savefig(Path(IMAGES_PATH) / 'grafico1.svg', bbox_inches='tight')

plt.show()
# %% GRÁFICO 2
# Gráfico com a proporção dos Desembolsos do BNDES para Indústria vis a vis Valor Adicionado da Indústria

# Carregando os dados
df_pib_hab = pq.read_table(Path(FINAL_DATA_PATH) / 'painel1.parquet').to_pandas()
df_bndes_industria = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_industria.parquet').to_pandas()

# Agrupando os dados por ano
va_industria_ano = df_pib_hab.groupby('ano')['va_industria_corrente'].sum().reset_index()
bndes_industria_ano = df_bndes_industria.groupby('ano')['desembolsos_industria_corrente'].sum().reset_index()

# Garantir compatibilidade de tipos para merge
va_industria_ano['ano'] = va_industria_ano['ano'].astype(str)
bndes_industria_ano['ano'] = bndes_industria_ano['ano'].astype(str)

# Unir os dados
df_merged = pd.merge(va_industria_ano, bndes_industria_ano, left_on='ano', right_on='ano', how='inner')

# Calcular proporção BNDES/PIB em percentual
df_merged['BNDES_pct_va_Industria'] = (df_merged['desembolsos_industria_corrente'] / df_merged['va_industria_corrente']) * 100

# Converter ano para inteiro para facilitar visualização
df_merged['ano'] = pd.to_numeric(df_merged['ano'], errors='coerce').astype('int64')

# Criar o gráfico de proporção BNDES/PIB
plt.plot(df_merged['ano'], df_merged['BNDES_pct_va_Industria'], 
         marker='o', linewidth=2.5, markersize=7,
         color='blue', label='BNDES/VA Indústria')

# Preencher área abaixo da linha
plt.fill_between(df_merged['ano'], df_merged['BNDES_pct_va_Industria'], 
                 alpha=0.1, color='gray')

# Linha de média
media = df_merged['BNDES_pct_va_Industria'].mean()
plt.axhline(y=media, color='red', linestyle=':', linewidth=2,
            label=f'Média: {media:.2f}%')

# Destacar período da crise financeira global
plt.axvspan(2008, 2010, alpha=0.15, color='orange', label='Crise Financeira Global')

# Configurações do gráfico
plt.xlabel('Ano')
plt.xticks(df_merged['ano']) # Ajustar valores de ano para inteiro sem casas decimais no gráfico
plt.ylabel('Desembolsos BNDES (% do VA Indústria)')
plt.title('Proporção dos Desembolsos do BNDES para o Setor Industrial\n em relação ao Valor Adicionado da Indústria')
plt.legend(loc='upper left')
plt.tight_layout()

# Salvar o gráfico como SVG para alta qualidade e escalabilidade
plt.savefig(Path(IMAGES_PATH) / 'grafico2.svg', bbox_inches='tight')

plt.show()
# %% GRÁFICO 3
# Gráfico com a proporção dos Desembolsos do BNDES para Agropecuária vis a vis Valor Adicionado da Agropecuária

# Carregando os dados
df_pib_hab = pq.read_table(Path(FINAL_DATA_PATH) / 'painel1.parquet').to_pandas()
df_bndes_agropecuaria = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_agropecuaria.parquet').to_pandas()

# Agrupando os dados por ano
va_agropecuaria_ano = df_pib_hab.groupby('ano')['va_agropecuaria_corrente'].sum().reset_index()
bndes_agropecuaria_ano = df_bndes_agropecuaria.groupby('ano')['desembolsos_agropecuaria_corrente'].sum().reset_index()

# Garantir compatibilidade de tipos para merge
va_agropecuaria_ano['ano'] = va_agropecuaria_ano['ano'].astype(str)
bndes_agropecuaria_ano['ano'] = bndes_agropecuaria_ano['ano'].astype(str)

# Unir os dados
df_merged = pd.merge(va_agropecuaria_ano, bndes_agropecuaria_ano, left_on='ano', right_on='ano', how='inner')

# Calcular proporção BNDES/PIB em percentual
df_merged['BNDES_pct_va_agropecuaria'] = (df_merged['desembolsos_agropecuaria_corrente'] / df_merged['va_agropecuaria_corrente']) * 100

# Converter ano para inteiro para facilitar visualização
df_merged['ano'] = pd.to_numeric(df_merged['ano'], errors='coerce').astype('int64')

# Criar o gráfico de proporção BNDES/PIB
plt.plot(df_merged['ano'], df_merged['BNDES_pct_va_agropecuaria'], 
         marker='o', linewidth=2.5, markersize=7,
         color='blue', label='BNDES/VA Agropecuária')

# Preencher área abaixo da linha
plt.fill_between(df_merged['ano'], df_merged['BNDES_pct_va_agropecuaria'], 
                 alpha=0.1, color='gray')

# Linha de média
media = df_merged['BNDES_pct_va_agropecuaria'].mean()
plt.axhline(y=media, color='red', linestyle=':', linewidth=2,
            label=f'Média: {media:.2f}%')

# Destacar período da seca no Nordeste
plt.axvspan(2012, 2013, alpha=0.15, color='orange', label='Crise Hidrológica no Nordeste')

# Configurações do gráfico
plt.xlabel('Ano')
plt.xticks(df_merged['ano']) # Ajustar valores de ano para inteiro sem casas decimais no gráfico
plt.ylabel('Desembolsos BNDES (% do VA Agropecuária)')
plt.title('Proporção dos Desembolsos do BNDES para o Setor Agropecuário\n em relação ao Valor Adicionado da Agropecuária')
plt.legend(loc='upper left')
plt.tight_layout()

# Salvar o gráfico como SVG para alta qualidade e escalabilidade
plt.savefig(Path(IMAGES_PATH) / 'grafico3.svg', bbox_inches='tight')

plt.show()

# %% GRÁFICO 4 ATÉ 11
# Gráficos de coeficientes por lag com IC 95% para os modelos principais e de robustez, tanto para indústria quanto para agropecuária
# Gráfico de coeficientes por lag com IC 95% - MODELO 1 - PIB real

df = pd.read_parquet(Path(REGRESSION_TABLES_PATH) / 'model1_pib_principal_uf_cluster_coef.parquet')

# Manter apenas variáveis de desembolso
df = df[df["var"].astype(str).str.startswith("share_desembolso")].copy()

# Identificar horizonte temporal
df["h"] = 0
df.loc[df["var"].str.contains("_lag"), "h"] = (df.loc[df["var"].str.contains("_lag"), "var"].str.extract(r"_lag(\d+)")[0].astype(int)* -1)
df.loc[df["var"].str.contains("_lead"), "h"] = (df.loc[df["var"].str.contains("_lead"), "var"].str.extract(r"_lead(\d+)")[0].astype(int))
df = df.sort_values("h")

# Criar labels do eixo x
df["h_label"] = df["h"].apply(lambda x: "t" if x == 0 else (f"t{x}" if x < 0 else f"t+{x}"))

# PLOT
fig, ax = plt.subplots()

# Sombreado laranja no horizonte t (contemporâneo)
ax.axvspan(-0.3, 0.3, alpha=0.15, color='orange', label='Período Contemporâneo (t)')

# Marcadores: sólidos para coef > 0, vazios para coef < 0
for idx, row in df.iterrows():
    marker = 'o' if row['coef'] >= 0 else 'o'
    fillstyle = 'full' if row['coef'] >= 0 else 'none'
    ax.plot(row['h'], row['coef'], marker=marker, fillstyle=fillstyle, 
            markersize=8, color='blue', markeredgewidth=1.5, markeredgecolor='blue')

# Linha conectando os pontos
ax.plot(df["h"], df["coef"], linewidth=1.5, color="blue", label="Coeficiente Δlog estimado")

# Linha zero
ax.axhline(0, linewidth=1.5, color="red", linestyle=":")

# Intervalo de confiança 95%
ax.errorbar(df["h"], df["coef"], yerr=[df["coef"] - df["ci_low"], df["ci_high"] - df["coef"]], 
            fmt="none", capsize=4, linewidth=2.5, color="blue", alpha=0.3)

ax.set_title("Dinâmica do efeito dos desembolsos sobre Δlog(PIB)\nFE: Município+Ano | SE: Cluster UF")
ax.set_xlabel("Horizonte")
ax.set_ylabel("Coeficiente (IC 95%)")
ax.set_xticks(df["h"])
ax.set_xticklabels(df["h_label"])
plt.legend(loc='upper left')
sns.despine()
plt.tight_layout()

# Salvar
plt.savefig(Path(IMAGES_PATH) / 'grafico4.svg', bbox_inches='tight')

plt.show()

# Gráfico de coeficientes por lag com IC 95% - MODELO 1 Complementar - PIB real

df = pd.read_parquet(Path(REGRESSION_TABLES_PATH) / 'model1_pib_complementar_uf_cluster_coef.parquet')

# Manter apenas variáveis de desembolso
df = df[df["var"].astype(str).str.startswith("share_desembolso")].copy()

# Identificar horizonte temporal
df["h"] = 0
df.loc[df["var"].str.contains("_lag"), "h"] = (df.loc[df["var"].str.contains("_lag"), "var"].str.extract(r"_lag(\d+)")[0].astype(int)* -1)
df.loc[df["var"].str.contains("_lead"), "h"] = (df.loc[df["var"].str.contains("_lead"), "var"].str.extract(r"_lead(\d+)")[0].astype(int))
df = df.sort_values("h")

# Criar labels do eixo x
df["h_label"] = df["h"].apply(lambda x: "t" if x == 0 else (f"t{x}" if x < 0 else f"t+{x}"))

# PLOT
fig, ax = plt.subplots()

# Sombreado laranja no horizonte t (contemporâneo)
ax.axvspan(-0.3, 0.3, alpha=0.15, color='orange', label='Período Contemporâneo (t)')

# Marcadores: sólidos para coef > 0, vazios para coef < 0
for idx, row in df.iterrows():
    marker = 'o' if row['coef'] >= 0 else 'o'
    fillstyle = 'full' if row['coef'] >= 0 else 'none'
    ax.plot(row['h'], row['coef'], marker=marker, fillstyle=fillstyle, 
            markersize=8, color='blue', markeredgewidth=1.5, markeredgecolor='blue')

# Linha conectando os pontos
ax.plot(df["h"], df["coef"], linewidth=1.5, color="blue", label="Coeficiente Δlog estimado")

# Linha zero
ax.axhline(0, linewidth=1.5, color="red", linestyle=":")

# Intervalo de confiança 95%
ax.errorbar(df["h"], df["coef"], yerr=[df["coef"] - df["ci_low"], df["ci_high"] - df["coef"]], 
            fmt="none", capsize=4, linewidth=2.5, color="blue", alpha=0.3)

ax.set_title("Dinâmica do efeito dos desembolsos sobre Δlog(PIB)\nFE: Município+Ano | SE: Cluster UF")
ax.set_xlabel("Horizonte")
ax.set_ylabel("Coeficiente (IC 95%)")
ax.set_xticks(df["h"])
ax.set_xticklabels(df["h_label"])
plt.legend(loc='upper left')
sns.despine()
plt.tight_layout()

# Salvar
plt.savefig(Path(IMAGES_PATH) / 'grafico5.svg', bbox_inches='tight')

plt.show()
# Gráfico de coeficientes por lag com IC 95% - MODELO 2  - Valor Adicionado Indústria real

df = pd.read_parquet(Path(REGRESSION_TABLES_PATH) / 'model2_va_industria_principal_uf_cluster_coef.parquet')

# Manter apenas variáveis de desembolso
df = df[df["var"].astype(str).str.startswith("share_desembolso")].copy()

# Identificar horizonte temporal
df["h"] = 0
df.loc[df["var"].str.contains("_lag"), "h"] = (df.loc[df["var"].str.contains("_lag"), "var"].str.extract(r"_lag(\d+)")[0].astype(int)* -1)
df.loc[df["var"].str.contains("_lead"), "h"] = (df.loc[df["var"].str.contains("_lead"), "var"].str.extract(r"_lead(\d+)")[0].astype(int))
df = df.sort_values("h")

# Criar labels do eixo x
df["h_label"] = df["h"].apply(lambda x: "t" if x == 0 else (f"t{x}" if x < 0 else f"t+{x}"))

# PLOT
fig, ax = plt.subplots()

# Sombreado laranja no horizonte t (contemporâneo)
ax.axvspan(-0.3, 0.3, alpha=0.15, color='orange', label='Período Contemporâneo (t)')

# Marcadores: sólidos para coef > 0, vazios para coef < 0
for idx, row in df.iterrows():
    marker = 'o' if row['coef'] >= 0 else 'o'
    fillstyle = 'full' if row['coef'] >= 0 else 'none'
    ax.plot(row['h'], row['coef'], marker=marker, fillstyle=fillstyle, 
            markersize=8, color='blue', markeredgewidth=1.5, markeredgecolor='blue')

# Linha conectando os pontos
ax.plot(df["h"], df["coef"], linewidth=1.5, color="blue", label="Coeficiente Δasinh estimado")

# Linha zero
ax.axhline(0, linewidth=1.5, color="red", linestyle=":")

# Intervalo de confiança 95%
ax.errorbar(df["h"], df["coef"], yerr=[df["coef"] - df["ci_low"], df["ci_high"] - df["coef"]], 
            fmt="none", capsize=4, linewidth=2.5, color="blue", alpha=0.3)

ax.set_title("Dinâmica do efeito dos desembolsos sobre Δasinh(Valor Adicionado Indústria)\nFE: Município+Ano | SE: Cluster UF")
ax.set_xlabel("Horizonte")
ax.set_ylabel("Coeficiente (IC 95%)")
ax.set_xticks(df["h"])
ax.set_xticklabels(df["h_label"])
plt.legend(loc='upper left')
sns.despine()
plt.tight_layout()

# Salvar
plt.savefig(Path(IMAGES_PATH) / 'grafico6.svg', bbox_inches='tight')

plt.show()

# Gráfico de coeficientes por lag com IC 95% - MODELO 2 Complementar - Valor Adicionado Indústria real

df = pd.read_parquet(Path(REGRESSION_TABLES_PATH) / 'model2_va_industria_complementar_uf_cluster_coef.parquet')

# Manter apenas variáveis de desembolso
df = df[df["var"].astype(str).str.startswith("share_desembolso")].copy()

# Identificar horizonte temporal
df["h"] = 0
df.loc[df["var"].str.contains("_lag"), "h"] = (df.loc[df["var"].str.contains("_lag"), "var"].str.extract(r"_lag(\d+)")[0].astype(int)* -1)
df.loc[df["var"].str.contains("_lead"), "h"] = (df.loc[df["var"].str.contains("_lead"), "var"].str.extract(r"_lead(\d+)")[0].astype(int))
df = df.sort_values("h")

# Criar labels do eixo x
df["h_label"] = df["h"].apply(lambda x: "t" if x == 0 else (f"t{x}" if x < 0 else f"t+{x}"))

# PLOT
fig, ax = plt.subplots()

# Sombreado laranja no horizonte t (contemporâneo)
ax.axvspan(-0.3, 0.3, alpha=0.15, color='orange', label='Período Contemporâneo (t)')

# Marcadores: sólidos para coef > 0, vazios para coef < 0
for idx, row in df.iterrows():
    marker = 'o' if row['coef'] >= 0 else 'o'
    fillstyle = 'full' if row['coef'] >= 0 else 'none'
    ax.plot(row['h'], row['coef'], marker=marker, fillstyle=fillstyle, 
            markersize=8, color='blue', markeredgewidth=1.5, markeredgecolor='blue')

# Linha conectando os pontos
ax.plot(df["h"], df["coef"], linewidth=1.5, color="blue", label="Coeficiente Δasinh estimado")

# Linha zero
ax.axhline(0, linewidth=1.5, color="red", linestyle=":")

# Intervalo de confiança 95%
ax.errorbar(df["h"], df["coef"], yerr=[df["coef"] - df["ci_low"], df["ci_high"] - df["coef"]], 
            fmt="none", capsize=4, linewidth=2.5, color="blue", alpha=0.3)

ax.set_title("Dinâmica do efeito dos desembolsos sobre Δasinh(Valor Adicionado Indústria)\nFE: Município+Ano | SE: Cluster UF")
ax.set_xlabel("Horizonte")
ax.set_ylabel("Coeficiente (IC 95%)")
ax.set_xticks(df["h"])
ax.set_xticklabels(df["h_label"])
plt.legend(loc='upper left')
sns.despine()
plt.tight_layout()

# Salvar
plt.savefig(Path(IMAGES_PATH) / 'grafico7.svg', bbox_inches='tight')

plt.show()

# Gráfico de coeficientes por lag com IC 95% - MODELO 3 - Valor Adicionado Agropecuária real

df = pd.read_parquet(Path(REGRESSION_TABLES_PATH) / 'model3_va_agropecuaria_principal_uf_cluster_coef.parquet')

# Manter apenas variáveis de desembolso
df = df[df["var"].astype(str).str.startswith("share_desembolso")].copy()

# Identificar horizonte temporal
df["h"] = 0
df.loc[df["var"].str.contains("_lag"), "h"] = (df.loc[df["var"].str.contains("_lag"), "var"].str.extract(r"_lag(\d+)")[0].astype(int)* -1)
df.loc[df["var"].str.contains("_lead"), "h"] = (df.loc[df["var"].str.contains("_lead"), "var"].str.extract(r"_lead(\d+)")[0].astype(int))
df = df.sort_values("h")

# Criar labels do eixo x
df["h_label"] = df["h"].apply(lambda x: "t" if x == 0 else (f"t{x}" if x < 0 else f"t+{x}"))

# PLOT
fig, ax = plt.subplots()

# Sombreado laranja no horizonte t (contemporâneo)
ax.axvspan(-0.3, 0.3, alpha=0.15, color='orange', label='Período Contemporâneo (t)')

# Marcadores: sólidos para coef > 0, vazios para coef < 0
for idx, row in df.iterrows():
    marker = 'o' if row['coef'] >= 0 else 'o'
    fillstyle = 'full' if row['coef'] >= 0 else 'none'
    ax.plot(row['h'], row['coef'], marker=marker, fillstyle=fillstyle, 
            markersize=8, color='blue', markeredgewidth=1.5, markeredgecolor='blue')

# Linha conectando os pontos
ax.plot(df["h"], df["coef"], linewidth=1.5, color="blue", label="Coeficiente Δasinh estimado")

# Linha zero
ax.axhline(0, linewidth=1.5, color="red", linestyle=":")

# Intervalo de confiança 95%
ax.errorbar(df["h"], df["coef"], yerr=[df["coef"] - df["ci_low"], df["ci_high"] - df["coef"]], 
            fmt="none", capsize=4, linewidth=2.5, color="blue", alpha=0.3)

ax.set_title("Dinâmica do efeito dos desembolsos sobre Δasinh(Valor Adicionado Agropecuária)\nFE: Município+Ano | SE: Cluster UF")
ax.set_xlabel("Horizonte")
ax.set_ylabel("Coeficiente (IC 95%)")
ax.set_xticks(df["h"])
ax.set_xticklabels(df["h_label"])
plt.legend(loc='upper left')
sns.despine()
plt.tight_layout()

# Salvar
plt.savefig(Path(IMAGES_PATH) / 'grafico8.svg', bbox_inches='tight')

plt.show()

# Gráfico de coeficientes por lag com IC 95% - MODELO 3 Complementar - Valor Adicionado Agropecuária real

df = pd.read_parquet(Path(REGRESSION_TABLES_PATH) / 'model3_va_agropecuaria_complementar_uf_cluster_coef.parquet')

# Manter apenas variáveis de desembolso
df = df[df["var"].astype(str).str.startswith("share_desembolso")].copy()

# Identificar horizonte temporal
df["h"] = 0
df.loc[df["var"].str.contains("_lag"), "h"] = (df.loc[df["var"].str.contains("_lag"), "var"].str.extract(r"_lag(\d+)")[0].astype(int)* -1)
df.loc[df["var"].str.contains("_lead"), "h"] = (df.loc[df["var"].str.contains("_lead"), "var"].str.extract(r"_lead(\d+)")[0].astype(int))
df = df.sort_values("h")

# Criar labels do eixo x
df["h_label"] = df["h"].apply(lambda x: "t" if x == 0 else (f"t{x}" if x < 0 else f"t+{x}"))

# PLOT
fig, ax = plt.subplots()

# Sombreado laranja no horizonte t (contemporâneo)
ax.axvspan(-0.3, 0.3, alpha=0.15, color='orange', label='Período Contemporâneo (t)')

# Marcadores: sólidos para coef > 0, vazios para coef < 0
for idx, row in df.iterrows():
    marker = 'o' if row['coef'] >= 0 else 'o'
    fillstyle = 'full' if row['coef'] >= 0 else 'none'
    ax.plot(row['h'], row['coef'], marker=marker, fillstyle=fillstyle, 
            markersize=8, color='blue', markeredgewidth=1.5, markeredgecolor='blue')

# Linha conectando os pontos
ax.plot(df["h"], df["coef"], linewidth=1.5, color="blue", label="Coeficiente Δasinh estimado")

# Linha zero
ax.axhline(0, linewidth=1.5, color="red", linestyle=":")

# Intervalo de confiança 95%
ax.errorbar(df["h"], df["coef"], yerr=[df["coef"] - df["ci_low"], df["ci_high"] - df["coef"]], 
            fmt="none", capsize=4, linewidth=2.5, color="blue", alpha=0.3)

ax.set_title("Dinâmica do efeito dos desembolsos sobre Δasinh(Valor Adicionado Agropecuária)\nFE: Município+Ano | SE: Cluster UF")
ax.set_xlabel("Horizonte")
ax.set_ylabel("Coeficiente (IC 95%)")
ax.set_xticks(df["h"])
ax.set_xticklabels(df["h_label"])
plt.legend(loc='upper left')
sns.despine()
plt.tight_layout()

# Salvar
plt.savefig(Path(IMAGES_PATH) / 'grafico9.svg', bbox_inches='tight')

plt.show()

# Gráfico de coeficientes por lag com IC 95% - MODELO 4 - PIBpc real

df = pd.read_parquet(Path(REGRESSION_TABLES_PATH) / 'model4_pibpc_principal_uf_cluster_coef.parquet')

# Manter apenas variáveis de desembolso
df = df[df["var"].astype(str).str.startswith("share_desembolso")].copy()

# Identificar horizonte temporal
df["h"] = 0
df.loc[df["var"].str.contains("_lag"), "h"] = (df.loc[df["var"].str.contains("_lag"), "var"].str.extract(r"_lag(\d+)")[0].astype(int)* -1)
df.loc[df["var"].str.contains("_lead"), "h"] = (df.loc[df["var"].str.contains("_lead"), "var"].str.extract(r"_lead(\d+)")[0].astype(int))
df = df.sort_values("h")

# Criar labels do eixo x
df["h_label"] = df["h"].apply(lambda x: "t" if x == 0 else (f"t{x}" if x < 0 else f"t+{x}"))

# PLOT
fig, ax = plt.subplots()

# Sombreado laranja no horizonte t (contemporâneo)
ax.axvspan(-0.3, 0.3, alpha=0.15, color='orange', label='Período Contemporâneo (t)')

# Marcadores: sólidos para coef > 0, vazios para coef < 0
for idx, row in df.iterrows():
    marker = 'o' if row['coef'] >= 0 else 'o'
    fillstyle = 'full' if row['coef'] >= 0 else 'none'
    ax.plot(row['h'], row['coef'], marker=marker, fillstyle=fillstyle, 
            markersize=8, color='blue', markeredgewidth=1.5, markeredgecolor='blue')

# Linha conectando os pontos
ax.plot(df["h"], df["coef"], linewidth=1.5, color="blue", label="Coeficiente Δlog estimado")

# Linha zero
ax.axhline(0, linewidth=1.5, color="red", linestyle=":")

# Intervalo de confiança 95%
ax.errorbar(df["h"], df["coef"], yerr=[df["coef"] - df["ci_low"], df["ci_high"] - df["coef"]], 
            fmt="none", capsize=4, linewidth=2.5, color="blue", alpha=0.3)

ax.set_title("Dinâmica do efeito dos desembolsos sobre Δlog(PIBpc)\nFE: Município+Ano | SE: Cluster UF")
ax.set_xlabel("Horizonte")
ax.set_ylabel("Coeficiente (IC 95%)")
ax.set_xticks(df["h"])
ax.set_xticklabels(df["h_label"])
plt.legend(loc='upper left')
sns.despine()
plt.tight_layout()

# Salvar
plt.savefig(Path(IMAGES_PATH) / 'grafico10.svg', bbox_inches='tight')

plt.show()

# Gráfico de coeficientes por lag com IC 95% - MODELO 4 Complementar - PIBpc real

df = pd.read_parquet(Path(REGRESSION_TABLES_PATH) / 'model4c_pibpc_complementar_uf_cluster_coef.parquet')

# Manter apenas variáveis de desembolso
df = df[df["var"].astype(str).str.startswith("share_desembolso")].copy()

# Identificar horizonte temporal
df["h"] = 0
df.loc[df["var"].str.contains("_lag"), "h"] = (df.loc[df["var"].str.contains("_lag"), "var"].str.extract(r"_lag(\d+)")[0].astype(int)* -1)
df.loc[df["var"].str.contains("_lead"), "h"] = (df.loc[df["var"].str.contains("_lead"), "var"].str.extract(r"_lead(\d+)")[0].astype(int))
df = df.sort_values("h")

# Criar labels do eixo x
df["h_label"] = df["h"].apply(lambda x: "t" if x == 0 else (f"t{x}" if x < 0 else f"t+{x}"))

# PLOT
fig, ax = plt.subplots()

# Sombreado laranja no horizonte t (contemporâneo)
ax.axvspan(-0.3, 0.3, alpha=0.15, color='orange', label='Período Contemporâneo (t)')

# Marcadores: sólidos para coef > 0, vazios para coef < 0
for idx, row in df.iterrows():
    marker = 'o' if row['coef'] >= 0 else 'o'
    fillstyle = 'full' if row['coef'] >= 0 else 'none'
    ax.plot(row['h'], row['coef'], marker=marker, fillstyle=fillstyle, 
            markersize=8, color='blue', markeredgewidth=1.5, markeredgecolor='blue')

# Linha conectando os pontos
ax.plot(df["h"], df["coef"], linewidth=1.5, color="blue", label="Coeficiente Δlog estimado")

# Linha zero
ax.axhline(0, linewidth=1.5, color="red", linestyle=":")

# Intervalo de confiança 95%
ax.errorbar(df["h"], df["coef"], yerr=[df["coef"] - df["ci_low"], df["ci_high"] - df["coef"]], 
            fmt="none", capsize=4, linewidth=2.5, color="blue", alpha=0.3)

ax.set_title("Dinâmica do efeito dos desembolsos sobre Δlog(PIBpc)\nFE: Município+Ano | SE: Cluster UF")
ax.set_xlabel("Horizonte")
ax.set_ylabel("Coeficiente (IC 95%)")
ax.set_xticks(df["h"])
ax.set_xticklabels(df["h_label"])
plt.legend(loc='upper left')
sns.despine()
plt.tight_layout()

# Salvar
plt.savefig(Path(IMAGES_PATH) / 'grafico5.svg', bbox_inches='tight')

plt.show()