# %% SCRIPT DE MODELO DE REGRESSÃO EFEITOS FIXOS (FIXED EFFECTS)
# Importando as bibliotecas necessárias
import pandas as pd
import json
from pathlib import Path
import pyarrow.parquet as pq
from linearmodels.panel import PanelOLS
from paths import FINAL_DATA_PATH, OUTPUTS_PATH, REGRESSION_TABLES_PATH, REGRESSION_MODELS_PATH, REGRESSION_TESTS_PATH

def salvar_resultados_panelols(res, *, model_name: str, out_dir, wald_tests: dict | None = None, overwrite: bool = True,) -> dict:
    """
    Salva resultados do PanelOLS (linearmodels) de forma estruturada.
    ----------
    res : linearmodels.panel.results.PanelEffectsResults -> Objeto retornado por PanelOLS(...).fit(...)
    model_name : str -> Nome curto do modelo
    out_dir : str | Path -> Diretório raiz onde os arquivos serão salvos.
    wald_tests : dict | None -> Dicionário {nome_teste: wald_obj} onde wald_obj é retorno de res.wald_test(...)
    overwrite : bool -> Se False, lança erro caso arquivos já existam.
    ----------
    Retorna
    dict com paths dos arquivos salvos.
    """
    out_dir = Path(out_dir)
    coef_path = Path(REGRESSION_TABLES_PATH) / f"{model_name}_coef.parquet"
    stats_path = Path(REGRESSION_MODELS_PATH) / f"{model_name}_stats.json"
    tests_path = Path(REGRESSION_TESTS_PATH) / f"{model_name}_wald_tests.parquet"

    if not overwrite:
        for p in (coef_path, stats_path, tests_path):
            if p.exists():
                raise FileExistsError(f"Arquivo já existe: {p}")

    # ---- Coeficientes (tabela longa, pronta para empilhar) ----
    params = res.params
    se = res.std_errors
    tstats = res.tstats
    pvals = res.pvalues

    # CI: tenta usar res.conf_int() se existir; senão calcula por aproximação normal (1.96)
    try:
        ci = res.conf_int()
        ci_low = ci.iloc[:, 0]
        ci_high = ci.iloc[:, 1]
    except Exception:
        ci_low = params - 1.96 * se
        ci_high = params + 1.96 * se

    df_coef = (
        pd.DataFrame(
            {
                "model": model_name,
                "var": params.index.astype(str),
                "coef": params.values,
                "std_err": se.values,
                "t": tstats.values,
                "p": pvals.values,
                "ci_low": ci_low.values,
                "ci_high": ci_high.values,
            }
        )
        .sort_values(["model", "var"])
        .reset_index(drop=True)
    )
    df_coef.to_parquet(coef_path, index=False)

    # ---- Estatísticas globais (json simples) ----
    def _safe(getter, default=None):
        try:
            return getter()
        except Exception:
            return default

    stats = {
        "model": model_name,
        "depvar": _safe(lambda: str(res.model.dependent.vars[0])),
        "nobs": _safe(lambda: int(res.nobs)),
        "entities": _safe(lambda: int(res.model.dependent.dataframe.index.levels[0].shape[0])),
        "time_periods": _safe(lambda: int(res.model.dependent.dataframe.index.levels[1].shape[0])),
        "rsq_within": _safe(lambda: float(res.rsquared_within)),
        "rsq_between": _safe(lambda: float(res.rsquared_between)),
        "rsq_overall": _safe(lambda: float(res.rsquared_overall)),
        "cov_type": _safe(lambda: str(res.cov_type)),
        "entity_effects": _safe(lambda: bool(getattr(res.model, "entity_effects", False))),
        "time_effects": _safe(lambda: bool(getattr(res.model, "time_effects", False))),
        "f_stat": _safe(lambda: float(res.f_statistic.stat)),
        "f_pval": _safe(lambda: float(res.f_statistic.pval)),
        "f_df_denom": _safe(lambda: int(res.f_statistic.df_denom)),
        "f_df_num": _safe(lambda: int(res.f_statistic.df_num)),
        "loglik": _safe(lambda: float(res.loglik)),
    }
    stats_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---- Wald tests (opcional; salva 1 linha por teste) ----
    saved_tests = False
    if wald_tests:
        rows = []
        for name, wt in wald_tests.items():
            if wt is None:
                continue

            df_val = getattr(wt, "df", None)
            # (mínimo) normaliza df para algo serializável no parquet
            if df_val is not None and not isinstance(df_val, (int, float, str)):
                try:
                    df_val = str(df_val)
                except Exception:
                    df_val = None
            
            rows.append(
                {
                    "model": model_name,
                    "test": name,
                    "stat": getattr(wt, "stat", None),
                    "pval": getattr(wt, "pval", None),
                    "df": getattr(wt, "df", None),
                }
            )
        if rows:
            pd.DataFrame(rows).to_parquet(tests_path, index=False)
            saved_tests = True

    return {
        "coef_parquet": str(coef_path),
        "stats_json": str(stats_path),
        "wald_parquet": str(tests_path) if saved_tests else None,
    }
# %% ANÁLISE 1 - Evolução do PIB real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)
# EQUAÇÃO DO MODELO 1: ΔlogPIB(it​) = β0​X(it) ​+ β1​X(i,t−1) ​+ β2​X(i,t−2) ​+ β3​X(i,t−3)​+ γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + γ4​logPOP(i,t−1​) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do PIB real
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do PIB real
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do PIB real
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do PIB real
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do PIB real ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Carregando Dataframe Painel1
df_model1 = pq.read_table(Path(FINAL_DATA_PATH) / 'painel1.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model1['ano'] = pd.to_numeric(df_model1['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model1['municipio_id'] = df_model1['codigo'].astype(str) + '-' + df_model1['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model1 = df_model1.set_index(['municipio_id', 'ano'])

# Variável dependente Y
y = df_model1['delta_log_pib_real']

# TESTES 1 ATÉ 4
# Variáveis independentes X (modelo principal)
X = df_model1[[
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res = mod.fit(
    cov_type='clustered',
    cluster_entity=True
)
print(f'Resumo do modelo de regressão com efeitos fixos duplos (municipais e de ano):')
print(res.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_real_pib_real_ano_anterior = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag2 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
hypothesis_acumulado = (
    'share_desembolso_real_pib_real_ano_anterior'
    ' + share_desembolso_real_pib_real_ano_anterior_lag1'
    ' + share_desembolso_real_pib_real_ano_anterior_lag2'
    ' + share_desembolso_real_pib_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2)

salvar_resultados_panelols(res, model_name="model1_pib_principal", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado": wald_test_2, }, overwrite=True,)

# SE Cluster por UF (Standard Errors por UF)
# Preparar clusters por UF - deve ter mesmo índice do painel
clusters_uf = df_model1[['estado']].copy()

# Rodar modelo com FE duplo e clusterização por UF
mod_uf = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_uf = mod_uf.fit(
    cov_type='clustered',
    clusters=clusters_uf
)
print(f'\nResumo do modelo de regressão com efeitos fixos duplos e SE clusterizados por UF:')
print(res_uf.summary)

# TESTE 5 (UF Cluster) - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
wald_test_1_uf = res_uf.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1_uf)

# TESTE 6 (UF Cluster) - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
wald_test_2_uf = res_uf.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2_uf)

salvar_resultados_panelols(res_uf, model_name="model1_pib_principal_uf_cluster", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1_uf, "wald_acumulado": wald_test_2_uf, }, overwrite=True,)

# MODELO COMPLEMENTAR - inclui leads para teste de causalidade reversa (placebo test)
# EQUAÇÃO DO MODELO 1: ΔlogPIB(it​) = ∑(k=0--3)​βk​X(i,t−k) ​+ θ1​X(i,t+1) ​+ θ2​X(i,t+2) + γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + γ4​logPOP(i,t−1​) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do PIB real
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do PIB real
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do PIB real
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do PIB real
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do PIB real ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 7: Teste de causalidade reversa (placebo test)
# H₀: θ1​ = θ2​ = Zero
# H₀: Suspeita de causalidade reversa.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Carregando Dataframe Painel1c
df_model1c = pq.read_table(Path(FINAL_DATA_PATH) / 'painel1c.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model1c['ano'] = pd.to_numeric(df_model1c['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model1c['municipio_id'] = df_model1c['codigo'].astype(str) + '-' + df_model1c['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model1c = df_model1c.set_index(['municipio_id', 'ano'])

# Variável dependente Y
y = df_model1c['delta_log_pib_real']

# TESTES 1 ATÉ 4
# Variáveis independentes X (modelo principal)
X = df_model1c[[
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
    'share_desembolso_real_pib_real_ano_anterior_lead1',
    'share_desembolso_real_pib_real_ano_anterior_lead2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res = mod.fit(
    cov_type='clustered',
    cluster_entity=True
)
print(f'Resumo do modelo de robustez com efeitos fixos duplos (municipais e de ano):')
print(res.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_real_pib_real_ano_anterior = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag2 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
hypothesis_acumulado = (
    'share_desembolso_real_pib_real_ano_anterior'
    ' + share_desembolso_real_pib_real_ano_anterior_lag1'
    ' + share_desembolso_real_pib_real_ano_anterior_lag2'
    ' + share_desembolso_real_pib_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2)

# TESTE 7 - Wald Test para os leads (pre-trend/antecipação) H0: θ1 = θ2 = 0
hypothesis_leads = [
    'share_desembolso_real_pib_real_ano_anterior_lead1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lead2 = 0',
]

wald_test_3 = res.wald_test(formula=hypothesis_leads) # type: ignore[arg-type]
print('Wald Test para os leads (H0: θ1 = θ2 = 0)')
print(wald_test_3)

salvar_resultados_panelols(res, model_name="model1c_pib_complementar", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado": wald_test_2, "wald_leads": wald_test_3}, overwrite=True,)

# SE Cluster por UF (Standard Errors por UF)
# Preparar clusters por UF - deve ter mesmo índice do painel
clusters_uf = df_model1c[['estado']].copy()

# Rodar modelo com FE duplo e clusterização por UF
mod_uf = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_uf = mod_uf.fit(
    cov_type='clustered',
    clusters=clusters_uf
)
print(f'\nResumo do modelo de robustez com efeitos fixos duplos e SE clusterizados por UF:')
print(res_uf.summary)

# TESTE 5 (UF Cluster) - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
wald_test_1_uf = res_uf.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1_uf)

# TESTE 6 (UF Cluster) - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
wald_test_2_uf = res_uf.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2_uf)

# TESTE 7 (UF Cluster) - Wald Test para os leads (pre-trend/antecipação) H0: θ1 = θ2 = 0
wald_test_3_uf = res_uf.wald_test(formula=hypothesis_leads) # type: ignore[arg-type]
print('\nWald Test (UF Cluster) para os leads (H0: θ1 = θ2 = 0)')
print(wald_test_3_uf)

salvar_resultados_panelols(res_uf, model_name="model1c_pib_complementar_uf_cluster", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1_uf, "wald_acumulado": wald_test_2_uf, "wald_leads": wald_test_3_uf}, overwrite=True,)

# %% ANÁLISE 2 - Evolução do Valor Adicionado para Indústria real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)
# EQUAÇÃO DO MODELO 2: ΔasinhVA_ind(it​) = β0​X(it) ​+ β1​X(i,t−1) ​+ β2​X(i,t−2) ​+ β3​X(i,t−3)​+ γ1PIBpc(i,t−1​) + γ2​share_industria(i,t−1) + γ3​share_agropecuaria(i,t−1) ​+ γ4​logPOP(i,t−1​) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES para setor industrial medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do Valor Adicionado pela Indústria real ao PIB ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Carregando Dataframe Painel2
df_model2 = pq.read_table(Path(FINAL_DATA_PATH) / 'painel2.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model2['ano'] = pd.to_numeric(df_model2['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model2['municipio_id'] = df_model2['codigo'].astype(str) + '-' + df_model2['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model2 = df_model2.set_index(['municipio_id', 'ano'])

# Variável dependente Y
y = df_model2['delta_asinh_va_industria_real']

# TESTES 1 ATÉ 4
# Variáveis independentes X (modelo principal)
X = df_model2[[
    'share_desembolso_industria_real_ano_anterior',
    'share_desembolso_industria_real_ano_anterior_lag1',
    'share_desembolso_industria_real_ano_anterior_lag2',
    'share_desembolso_industria_real_ano_anterior_lag3',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res = mod.fit(
    cov_type='clustered',
    cluster_entity=True
)
print(f'Resumo do modelo de regressão com efeitos fixos duplos (municipais e de ano):')
print(res.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_industria_real_ano_anterior = 0',
    'share_desembolso_industria_real_ano_anterior_lag1 = 0',
    'share_desembolso_industria_real_ano_anterior_lag2 = 0',
    'share_desembolso_industria_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
hypothesis_acumulado = (
    'share_desembolso_industria_real_ano_anterior'
    ' + share_desembolso_industria_real_ano_anterior_lag1'
    ' + share_desembolso_industria_real_ano_anterior_lag2'
    ' + share_desembolso_industria_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2)

salvar_resultados_panelols(res, model_name="model2_va_industria_principal", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado": wald_test_2, }, overwrite=True,)

# SE Cluster por UF (Standard Errors por UF)
# Preparar clusters por UF - deve ter mesmo índice do painel
clusters_uf = df_model2[['estado']].copy()

# Rodar modelo com FE duplo e clusterização por UF
mod_uf = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_uf = mod_uf.fit(
    cov_type='clustered',
    clusters=clusters_uf
)
print(f'\nResumo do modelo de regressão com efeitos fixos duplos e SE clusterizados por UF:')
print(res_uf.summary)

# TESTE 5 (UF Cluster) - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
wald_test_1_uf = res_uf.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1_uf)

# TESTE 6 (UF Cluster) - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
wald_test_2_uf = res_uf.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2_uf)

salvar_resultados_panelols(res_uf, model_name="model2_va_industria_principal_uf_cluster", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1_uf, "wald_acumulado": wald_test_2_uf, }, overwrite=True,)

# MODELO COMPLEMENTAR - inclui leads para teste de causalidade reversa (placebo test)
# EQUAÇÃO DO MODELO 2: ΔasinhVA_ind(it​) = ∑(k=0--3)​βk​X(i,t−k) ​+ θ1​X(i,t+1) ​+ θ2​X(i,t+2) + γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + γ4​logPOP(i,t−1​) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES para setor industrial medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do Valor Adicionado pela Indústria real ao PIB ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 7: Teste de causalidade reversa (placebo test)
# H₀: θ1​ = θ2​ = Zero
# H₀: Suspeita de causalidade reversa.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Carregando Dataframe Painel2c
df_model2c = pq.read_table(Path(FINAL_DATA_PATH) / 'painel2c.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model2c['ano'] = pd.to_numeric(df_model2c['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model2c['municipio_id'] = df_model2c['codigo'].astype(str) + '-' + df_model2c['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model2c = df_model2c.set_index(['municipio_id', 'ano'])

# Variável dependente Y
y = df_model2c['delta_asinh_va_industria_real']

# TESTES 1 ATÉ 4
# Variáveis independentes X (modelo principal)
X = df_model2c[[
    'share_desembolso_industria_real_ano_anterior',
    'share_desembolso_industria_real_ano_anterior_lag1',
    'share_desembolso_industria_real_ano_anterior_lag2',
    'share_desembolso_industria_real_ano_anterior_lag3',
    'share_desembolso_industria_real_pib_real_ano_anterior_lead1',
    'share_desembolso_industria_real_pib_real_ano_anterior_lead2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res = mod.fit(
    cov_type='clustered',
    cluster_entity=True
)
print(f'Resumo do modelo de robustez com efeitos fixos duplos (municipais e de ano):')
print(res.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_industria_real_ano_anterior = 0',
    'share_desembolso_industria_real_ano_anterior_lag1 = 0',
    'share_desembolso_industria_real_ano_anterior_lag2 = 0',
    'share_desembolso_industria_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
hypothesis_acumulado = (
    'share_desembolso_industria_real_ano_anterior'
    ' + share_desembolso_industria_real_ano_anterior_lag1'
    ' + share_desembolso_industria_real_ano_anterior_lag2'
    ' + share_desembolso_industria_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2)

# TESTE 7 - Wald Test para os leads (pre-trend/antecipação) H0: θ1 = θ2 = 0
hypothesis_leads = [
    'share_desembolso_industria_real_pib_real_ano_anterior_lead1 = 0',
    'share_desembolso_industria_real_pib_real_ano_anterior_lead2 = 0',
]

wald_test_3 = res.wald_test(formula=hypothesis_leads) # type: ignore[arg-type]
print('Wald Test para os leads (H0: θ1 = θ2 = 0)')
print(wald_test_3)

salvar_resultados_panelols(res, model_name="model2c_va_industria_complementar", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado": wald_test_2, "wald_leads": wald_test_3}, overwrite=True,)

# SE Cluster por UF (Standard Errors por UF)
# Preparar clusters por UF - deve ter mesmo índice do painel
clusters_uf = df_model2c[['estado']].copy()

# Rodar modelo com FE duplo e clusterização por UF
mod_uf = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_uf = mod_uf.fit(
    cov_type='clustered',
    clusters=clusters_uf
)
print(f'\nResumo do modelo de robustez com efeitos fixos duplos e SE clusterizados por UF:')
print(res_uf.summary)

# TESTE 5 (UF Cluster) - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
wald_test_1_uf = res_uf.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1_uf)

# TESTE 6 (UF Cluster) - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
wald_test_2_uf = res_uf.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2_uf)

# TESTE 7 (UF Cluster) - Wald Test para os leads (pre-trend/antecipação) H0: θ1 = θ2 = 0
wald_test_3_uf = res_uf.wald_test(formula=hypothesis_leads) # type: ignore[arg-type]
print('\nWald Test (UF Cluster) para os leads (H0: θ1 = θ2 = 0)')
print(wald_test_3_uf)

salvar_resultados_panelols(res_uf, model_name="model2c_va_industria_complementar_uf_cluster", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1_uf, "wald_acumulado": wald_test_2_uf, "wald_leads": wald_test_3_uf}, overwrite=True,)
# %% ANÁLISE 3 - Evolução do Valor Adicionado para Agropecuária real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)
# EQUAÇÃO DO MODELO 3: ΔasinhVA_agro(it​) = β0​X(it) ​+ β1​X(i,t−1) ​+ β2​X(i,t−2) ​+ β3​X(i,t−3)​+ γ1PIBpc(i,t−1​) + γ2​share_industria(i,t−1) + γ3​share_agropecuaria(i,t−1) ​+ γ4​logPOP(i,t−1​) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES para setor agropecuário medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do Valor Adicionado pela Agropecuária real ao PIB
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do Valor Adicionado pela Agropecuária real ao PIB
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Agropecuária real ao PIB
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Agropecuária real ao PIB
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do Valor Adicionado pela Agropecuária real ao PIB ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Carregando Dataframe Painel3
df_model3 = pq.read_table(Path(FINAL_DATA_PATH) / 'painel3.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model3['ano'] = pd.to_numeric(df_model3['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model3['municipio_id'] = df_model3['codigo'].astype(str) + '-' + df_model3['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model3 = df_model3.set_index(['municipio_id', 'ano'])

# Variável dependente Y
y = df_model3['delta_asinh_va_agropecuaria_real']

# TESTES 1 ATÉ 4
# Variáveis independentes X (modelo principal)
X = df_model3[[
    'share_desembolso_agropecuaria_real_ano_anterior',
    'share_desembolso_agropecuaria_real_ano_anterior_lag1',
    'share_desembolso_agropecuaria_real_ano_anterior_lag2',
    'share_desembolso_agropecuaria_real_ano_anterior_lag3',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res = mod.fit(
    cov_type='clustered',
    cluster_entity=True
)
print(f'Resumo do modelo de regressão com efeitos fixos duplos (municipais e de ano):')
print(res.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_agropecuaria_real_ano_anterior = 0',
    'share_desembolso_agropecuaria_real_ano_anterior_lag1 = 0',
    'share_desembolso_agropecuaria_real_ano_anterior_lag2 = 0',
    'share_desembolso_agropecuaria_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
hypothesis_acumulado = (
    'share_desembolso_agropecuaria_real_ano_anterior'
    ' + share_desembolso_agropecuaria_real_ano_anterior_lag1'
    ' + share_desembolso_agropecuaria_real_ano_anterior_lag2'
    ' + share_desembolso_agropecuaria_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2)

salvar_resultados_panelols(res, model_name="model3_va_agropecuaria_principal", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado": wald_test_2, }, overwrite=True,)

# SE Cluster por UF (Standard Errors por UF)
# Preparar clusters por UF - deve ter mesmo índice do painel
clusters_uf = df_model3[['estado']].copy()

# Rodar modelo com FE duplo e clusterização por UF
mod_uf = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_uf = mod_uf.fit(
    cov_type='clustered',
    clusters=clusters_uf
)
print(f'\nResumo do modelo de regressão com efeitos fixos duplos e SE clusterizados por UF:')
print(res_uf.summary)

# TESTE 5 (UF Cluster) - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
wald_test_1_uf = res_uf.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1_uf)

# TESTE 6 (UF Cluster) - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
wald_test_2_uf = res_uf.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2_uf)

salvar_resultados_panelols(res_uf, model_name="model3_va_agropecuaria_principal_uf_cluster", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1_uf, "wald_acumulado": wald_test_2_uf, }, overwrite=True,)

# MODELO COMPLEMENTAR - inclui leads para teste de causalidade reversa (placebo test)
# EQUAÇÃO DO MODELO 3: ΔasinhVA_agro(it​) = ∑(k=0--3)​βk​X(i,t−k) ​+ θ1​X(i,t+1) ​+ θ2​X(i,t+2) + γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + γ4​logPOP(i,t−1​) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES para setor agropecuário medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do Valor Adicionado pela Indústria real ao PIB ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 7: Teste de causalidade reversa (placebo test)
# H₀: θ1​ = θ2​ = Zero
# H₀: Suspeita de causalidade reversa.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Carregando Dataframe Painel3c
df_model3c = pq.read_table(Path(FINAL_DATA_PATH) / 'painel3c.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model3c['ano'] = pd.to_numeric(df_model3c['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model3c['municipio_id'] = df_model3c['codigo'].astype(str) + '-' + df_model3c['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model3c = df_model3c.set_index(['municipio_id', 'ano'])

# Variável dependente Y
y = df_model3c['delta_asinh_va_agropecuaria_real']

# TESTES 1 ATÉ 4
# Variáveis independentes X (modelo principal)
X = df_model3c[[
    'share_desembolso_agropecuaria_real_ano_anterior',
    'share_desembolso_agropecuaria_real_ano_anterior_lag1',
    'share_desembolso_agropecuaria_real_ano_anterior_lag2',
    'share_desembolso_agropecuaria_real_ano_anterior_lag3',
    'share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead1',
    'share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res = mod.fit(
    cov_type='clustered',
    cluster_entity=True
)
print(f'Resumo do modelo de robustez com efeitos fixos duplos (municipais e de ano):')
print(res.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_agropecuaria_real_ano_anterior = 0',
    'share_desembolso_agropecuaria_real_ano_anterior_lag1 = 0',
    'share_desembolso_agropecuaria_real_ano_anterior_lag2 = 0',
    'share_desembolso_agropecuaria_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
hypothesis_acumulado = (
    'share_desembolso_agropecuaria_real_ano_anterior'
    ' + share_desembolso_agropecuaria_real_ano_anterior_lag1'
    ' + share_desembolso_agropecuaria_real_ano_anterior_lag2'
    ' + share_desembolso_agropecuaria_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2)

# TESTE 7 - Wald Test para os leads (pre-trend/antecipação) H0: θ1 = θ2 = 0
hypothesis_leads = [
    'share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead1 = 0',
    'share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead2 = 0',
]

wald_test_3 = res.wald_test(formula=hypothesis_leads) # type: ignore[arg-type]
print('Wald Test para os leads (H0: θ1 = θ2 = 0)')
print(wald_test_3)

salvar_resultados_panelols(res, model_name="model3c_va_agropecuaria_complementar", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado": wald_test_2, "wald_leads": wald_test_3}, overwrite=True,)

# SE Cluster por UF (Standard Errors por UF)
# Preparar clusters por UF - deve ter mesmo índice do painel
clusters_uf = df_model3c[['estado']].copy()

# Rodar modelo com FE duplo e clusterização por UF
mod_uf = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_uf = mod_uf.fit(
    cov_type='clustered',
    clusters=clusters_uf
)
print(f'\nResumo do modelo de robustez com efeitos fixos duplos e SE clusterizados por UF:')
print(res_uf.summary)

# TESTE 5 (UF Cluster) - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
wald_test_1_uf = res_uf.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1_uf)

# TESTE 6 (UF Cluster) - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
wald_test_2_uf = res_uf.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2_uf)

# TESTE 7 (UF Cluster) - Wald Test para os leads (pre-trend/antecipação) H0: θ1 = θ2 = 0
wald_test_3_uf = res_uf.wald_test(formula=hypothesis_leads) # type: ignore[arg-type]
print('\nWald Test (UF Cluster) para os leads (H0: θ1 = θ2 = 0)')
print(wald_test_3_uf)

salvar_resultados_panelols(res_uf, model_name="model3c_va_agropecuaria_complementar_uf_cluster", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1_uf, "wald_acumulado": wald_test_2_uf, "wald_leads": wald_test_3_uf}, overwrite=True,)
# %% ANÁLISE 4 - Evolução do PIB per capita ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)
# EQUAÇÃO DO MODELO 4: Δlog(PIBpc(it)) = β0​X(it) ​+ β1​X(i,t−1) ​+ β2​X(i,t−2) ​+ β3​X(i,t−3)​+ γ1PIBpc(i,t−1​) + γ2​share_industria(i,t−1) + γ3​share_agropecuaria(i,t−1) ​+ α(i) ​+ λ(t)​ + ε(it)​
#!ATENÇÃO: log(popi,t-1) foi removida do modelo, por causar redundância estrutural - já se encontra no denominador do PIB per capita
#  X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do Valor Adicionado pela Agropecuária real ao PIB
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do Valor Adicionado pela Agropecuária real ao PIB
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Agropecuária real ao PIB
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Agropecuária real ao PIB
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Agropecuária real ao PIB e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do Valor Adicionado pela Agropecuária real ao PIB ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Carregando Dataframe Painel4
df_model4 = pq.read_table(Path(FINAL_DATA_PATH) / 'painel4.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model4['ano'] = pd.to_numeric(df_model4['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model4['municipio_id'] = df_model4['codigo'].astype(str) + '-' + df_model4['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model4 = df_model4.set_index(['municipio_id', 'ano'])

# Variável dependente Y
y = df_model4['delta_log_pibpc_real']

# TESTES 1 ATÉ 4
# Variáveis independentes X (modelo principal)
X = df_model4[[
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1'
]]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res = mod.fit(
    cov_type='clustered',
    cluster_entity=True
)
print(f'Resumo do modelo de regressão com efeitos fixos duplos (municipais e de ano):')
print(res.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_real_pib_real_ano_anterior = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag2 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
hypothesis_acumulado = (
    'share_desembolso_real_pib_real_ano_anterior'
    ' + share_desembolso_real_pib_real_ano_anterior_lag1'
    ' + share_desembolso_real_pib_real_ano_anterior_lag2'
    ' + share_desembolso_real_pib_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2)

salvar_resultados_panelols(res, model_name="model4_pibpc_principal", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado": wald_test_2, }, overwrite=True,)

# SE Cluster por UF (Standard Errors por UF)
# Preparar clusters por UF - deve ter mesmo índice do painel
clusters_uf = df_model4[['estado']].copy()

# Rodar modelo com FE duplo e clusterização por UF
mod_uf = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_uf = mod_uf.fit(
    cov_type='clustered',
    clusters=clusters_uf
)
print(f'\nResumo do modelo de regressão com efeitos fixos duplos e SE clusterizados por UF:')
print(res_uf.summary)

# TESTE 5 (UF Cluster) - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
wald_test_1_uf = res_uf.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1_uf)

# TESTE 6 (UF Cluster) - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
wald_test_2_uf = res_uf.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2_uf)

salvar_resultados_panelols(res_uf, model_name="model4_pibpc_principal_uf_cluster", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1_uf, "wald_acumulado": wald_test_2_uf, }, overwrite=True,)

# MODELO COMPLEMENTAR - inclui leads para teste de causalidade reversa (placebo test)
# EQUAÇÃO DO MODELO 4: Δlog(PIBpc(it​)) = ∑(k=0--3)​βk​X(i,t−k) ​+ θ1​X(i,t+1) ​+ θ2​X(i,t+2) + γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + α(i) ​+ λ(t)​ + ε(it)​
#!ATENÇÃO: log(popi,t-1) foi removida do modelo, por causar redundância estrutural - já se encontra no denominador do PIB per capita
#  X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do Valor Adicionado pela Indústria real ao PIB
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do Valor Adicionado pela Indústria real ao PIB e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do Valor Adicionado pela Indústria real ao PIB ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 7: Teste de causalidade reversa (placebo test)
# H₀: θ1​ = θ2​ = Zero
# H₀: Suspeita de causalidade reversa.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Carregando Dataframe Painel4c
df_model4c = pq.read_table(Path(FINAL_DATA_PATH) / 'painel4c.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model4c['ano'] = pd.to_numeric(df_model4c['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model4c['municipio_id'] = df_model4c['codigo'].astype(str) + '-' + df_model4c['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model4c = df_model4c.set_index(['municipio_id', 'ano'])

# Variável dependente Y
y = df_model4c['delta_log_pibpc_real']

# TESTES 1 ATÉ 4
# Variáveis independentes X (modelo principal)
X = df_model4c[[
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
    'share_desembolso_real_pib_real_ano_anterior_lead1',
    'share_desembolso_real_pib_real_ano_anterior_lead2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1'
]]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res = mod.fit(
    cov_type='clustered',
    cluster_entity=True
)
print(f'Resumo do modelo de robustez com efeitos fixos duplos (municipais e de ano):')
print(res.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_real_pib_real_ano_anterior = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag2 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
hypothesis_acumulado = (
    'share_desembolso_real_pib_real_ano_anterior'
    ' + share_desembolso_real_pib_real_ano_anterior_lag1'
    ' + share_desembolso_real_pib_real_ano_anterior_lag2'
    ' + share_desembolso_real_pib_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2)

# TESTE 7 - Wald Test para os leads (pre-trend/antecipação) H0: θ1 = θ2 = 0
hypothesis_leads = [
    'share_desembolso_real_pib_real_ano_anterior_lead1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lead2 = 0',
]

wald_test_3 = res.wald_test(formula=hypothesis_leads) # type: ignore[arg-type]
print('Wald Test para os leads (H0: θ1 = θ2 = 0)')
print(wald_test_3)

salvar_resultados_panelols(res, model_name="model4c_pibpc_complementar", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado": wald_test_2, "wald_leads": wald_test_3}, overwrite=True,)

# SE Cluster por UF (Standard Errors por UF)
# Preparar clusters por UF - deve ter mesmo índice do painel
clusters_uf = df_model4c[['estado']].copy()

# Rodar modelo com FE duplo e clusterização por UF
mod_uf = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_uf = mod_uf.fit(
    cov_type='clustered',
    clusters=clusters_uf
)
print(f'\nResumo do modelo de robustez com efeitos fixos duplos e SE clusterizados por UF:')
print(res_uf.summary)

# TESTE 5 (UF Cluster) - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
wald_test_1_uf = res_uf.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1_uf)

# TESTE 6 (UF Cluster) - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0
wald_test_2_uf = res_uf.wald_test(formula=hypothesis_acumulado) # type: ignore[arg-type]
print(f'\nWald Test (UF Cluster) para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0)')
print(wald_test_2_uf)

# TESTE 7 (UF Cluster) - Wald Test para os leads (pre-trend/antecipação) H0: θ1 = θ2 = 0
wald_test_3_uf = res_uf.wald_test(formula=hypothesis_leads) # type: ignore[arg-type]
print('\nWald Test (UF Cluster) para os leads (H0: θ1 = θ2 = 0)')
print(wald_test_3_uf)

salvar_resultados_panelols(res_uf, model_name="model4c_pibpc_complementar_uf_cluster", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1_uf, "wald_acumulado": wald_test_2_uf, "wald_leads": wald_test_3_uf}, overwrite=True,)
# %%
