# %% SCRIPT DE MODELO DE REGRESSÃO EFEITOS FIXOS (FIXED EFFECTS)
# Importando as bibliotecas necessárias
import pandas as pd
import numpy as np
import json
from pathlib import Path
import pyarrow.parquet as pq
from linearmodels.panel import PanelOLS
from paths import FINAL_DATA_PATH, OUTPUTS_PATH, REGRESSION_TABLES_PATH, REGRESSION_MODELS_PATH, REGRESSION_TESTS_PATH
from dataclasses import dataclass

@dataclass
class SimpleTest:
    stat: float | None
    pval: float | None
    df: int | None = 1

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

    # Coeficientes (tabela longa)
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

    # Estatísticas globais
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

    # Wald tests - salva 1 linha por teste
    saved_tests = False
    if wald_tests:
        rows = []
        for name, wt in wald_tests.items():
            if wt is None:
                continue

            df_val = getattr(wt, "df", None)
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
# %% CONFIGURAÇÃO DOS MODELOS

# MODELO A1.1 BASELINE
lhs_modelo_a1_1 = ['delta_log_pib_real']
rhs_modelo_a1_1 = [
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]

# MODELO A1.2 COMPARATIVO
lhs_modelo_a1_2 = ['delta_log_pibpc_real']
rhs_modelo_a1_2 = [
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1'
]

# MODELO A2.1 BASELINE COM 2 LEADS
lhs_modelo_a2_1 = ['delta_log_pib_real']
rhs_modelo_a2_1 = [
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
]

# MODELO A2.2 COMPARATIVO COM 2 LEADS
lhs_modelo_a2_2 = ['delta_log_pibpc_real']
rhs_modelo_a2_2 = [
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
    'share_desembolso_real_pib_real_ano_anterior_lead1',
    'share_desembolso_real_pib_real_ano_anterior_lead2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1'
]

# MODELO B1.1 CONTRACICLICO COM PIB REAL
lhs_modelo_b1_1 = ['share_desembolso_real_pib_real_ano_anterior']
rhs_modelo_b1_1 = [
    'delta_log_pib_real',
    'delta_log_pib_real_lag1',
    'delta_log_pib_real_lag2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]

# MODELO B2_IND SETORIAL
lhs_modelo_b2_1_ind = ['share_desembolso_industria_real_ano_anterior']
rhs_modelo_b2_1_ind = [
    'delta_asinh_va_industria_real',
    'delta_asinh_va_industria_real_lag1',
    'delta_asinh_va_industria_real_lag2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]

# MODELO B2_AGRO SETORIAL
lhs_modelo_b2_1_agro = ['share_desembolso_agropecuaria_real_ano_anterior']
rhs_modelo_b2_1_agro = [
    'delta_asinh_va_agropecuaria_real',
    'delta_asinh_va_agropecuaria_real_lag1',
    'delta_asinh_va_agropecuaria_real_lag2',
    'delta_log_pib_real_lag2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]

# MODELO B3 LINEAR PROBABILITY MODEL
lhs_modelo_b3 = [None]  # placeholder, será definido depois
rhs_modelo_b3 = [
    'delta_log_pib_real',
    'delta_log_pib_real_lag1',
    'delta_log_pib_real_lag2',
    'log_pibpc_real_lag1',
    'share_industria_lag1',
    'share_agropecuaria_lag1',
    'log_populacao_lag1'
]
# %% ANÁLISE 1 - MODELO A1.1 BASELINE
# MODELO A1.1 BASELINE - Evolução do PIB real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - FE 2-way (municípios e anos)
# EQUAÇÃO DO MODELO: ΔlogPIB(it​) = β0​X(it) ​+ β1​X(i,t−1) ​+ β2​X(i,t−2) ​+ β3​X(i,t−3)​+ γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + γ4​logPOP(i,t−1​) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
# SE cluster (município + ano)
# Valores deflacionados pelo PIB - em termos reais de 2021
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
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado bilateral
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do PIB real ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 7: Teste de efeito acumulado unilateral
# H₀: ∑(K=0--3)​β(k)​ <= Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do PIB real ao longo dos quatro períodos considerados é estatisticamente nulo ou inferior a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Carregando Dataframe Painel1
df_model = pq.read_table(Path(FINAL_DATA_PATH) / 'painel1.parquet').to_pandas()

# Converter Ano para numérico (requerido por PanelOLS)
df_model['ano'] = pd.to_numeric(df_model['ano'], errors='coerce')

# Criar identificador único de município (Código-Estado)
df_model['municipio_id'] = df_model['codigo'].astype(str) + '-' + df_model['estado'].astype(str)

# Configurando o índice do painel com 2 níveis: (municipio_id, Ano)
df_model = df_model.set_index(['municipio_id', 'ano'])

# Selecionar dados de interesse para a regressão antes de dropNA (para manter o máximo de observações possível)
df_model_a1_1 = df_model[lhs_modelo_a1_1 + rhs_modelo_a1_1].copy().dropna()

# Variável dependente Y
y = df_model_a1_1[lhs_modelo_a1_1[0]]

# Variáveis independentes X (modelo principal)
X = df_model_a1_1[rhs_modelo_a1_1]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_a1_1 = mod.fit(
    cov_type='clustered',
    cluster_entity=True,
    cluster_time=True
)

print(f'Resumo do MODELO A1.1 BASELINE de regressão com efeitos fixos duplos (municipais e de ano) - SE clusterizados por município e ano:')
print(res_a1_1.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_real_pib_real_ano_anterior = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag2 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res_a1_1.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0 bilateral
hypothesis_acumulado_bi = (
    'share_desembolso_real_pib_real_ano_anterior'
    ' + share_desembolso_real_pib_real_ano_anterior_lag1'
    ' + share_desembolso_real_pib_real_ano_anterior_lag2'
    ' + share_desembolso_real_pib_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res_a1_1.wald_test(formula=hypothesis_acumulado_bi) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0 bilateral)')
print(wald_test_2)

# TESTE 7 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0 unilateral
p_bilateral = float(wald_test_2.pval)

# nomes dos coeficientes que entram na soma
beta_names = [
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
]

# soma pontual (R*b)
beta_sum = float(res_a1_1.params[beta_names].sum())

# matriz de covariância dos estimadores
V = res_a1_1.cov  # pandas DataFrame (p x p)

# variância da soma: 1' V 1, restrita ao sub-bloco dos betas
V_sub = V.loc[beta_names, beta_names].to_numpy()
var_sum = float(np.ones(4) @ V_sub @ np.ones(4))
se_sum = float(np.sqrt(var_sum))

# t-stat do acumulado
t_sum = beta_sum / se_sum

# p-valor unilateral derivado do Wald bilateral (igualdade), usando sinal
p_bilateral = float(wald_test_2.pval)
if beta_sum > 0:
    p_unilateral = p_bilateral / 2
else:
    p_unilateral = 1 - (p_bilateral / 2)

# empacotar para salvar
wald_acumulado_t_uni = SimpleTest(stat=float(t_sum), pval=float(p_unilateral), df=1)

print("Acumulado:")
print("soma dos betas:", beta_sum)
print("se(soma):", se_sum)
print("t(soma):", t_sum)
print("p unilateral (H1: soma > 0):", p_unilateral)

salvar_resultados_panelols(res_a1_1, model_name="model_a1_1", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado_bi": wald_test_2, "wald_acumulado_uni": wald_acumulado_t_uni}, overwrite=True,)

# %% ANÁLISE 2 - MODELO A1.2
# MODELO A1.2 COMPARATIVO - ΔlogPIBpc(it​)
# EQUAÇÃO DO MODELO: ΔlogPIBpc(it​) = ∑(k=0--3)​βk​X(i,t−k) ​+ γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Efeito contemporâneo dos desembolsos do BNDES no crescimento do PIB per capita real
# H₀: β0​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB per capita real no período t e o desembolso do BNDES no período t (medido como proporção do PIB do período anterior), controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano dos desembolsos do BNDES no crescimento do PIB per capita real
# H₀: β1​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB per capita real no período t e o desembolso do BNDES realizado no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos dos desembolsos do BNDES no crescimento do PIB per capita real
# H₀: β2​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB per capita real no período t e o desembolso do BNDES realizado no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: Efeito defasado de três anos dos desembolsos do BNDES no crescimento do PIB per capita real
# H₀: β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB per capita real no período t e o desembolso do BNDES realizado no período t−3, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: Wald Test para os coeficientes de interesse
# H₀: β0 = β1 = β2 = β3 = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB per capita real e os desembolsos do BNDES em nenhum dos períodos considerados (t, t−1, t−2 e t−3), após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 6: Teste de efeito acumulado bilateral
# H₀: ∑(K=0--3)​β(k)​ = Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do PIB real ao longo dos quatro períodos considerados é estatisticamente igual a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 7: Teste de efeito acumulado unilateral
# H₀: ∑(K=0--3)​β(k)​ <= Zero
# H₀: O efeito acumulado dos desembolsos do BNDES sobre o crescimento do PIB per capita real ao longo dos quatro períodos considerados é estatisticamente nulo ou inferior a zero, após controle por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Selecionar dados de interesse para a regressão antes de dropNA (para manter o máximo de observações possível)
df_model_a1_2 = df_model[lhs_modelo_a1_2 + rhs_modelo_a1_2].copy().dropna()

# Variável dependente Y
y = df_model_a1_2[lhs_modelo_a1_2[0]]

# Variáveis independentes X (modelo principal)
X = df_model_a1_2[rhs_modelo_a1_2]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_a1_2 = mod.fit(
    cov_type='clustered',
    cluster_entity=True,
    cluster_time=True
)

print(f'Resumo do MODELO A1.2 Comparativo de regressão com efeitos fixos duplos (municipais e de ano) - SE clusterizados por município e ano:')
print(res_a1_2.summary)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​:β0​=β1​=β2​=β3​=0
hypothesis = [
    'share_desembolso_real_pib_real_ano_anterior = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag2 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lag3 = 0',
]

wald_test_1 = res_a1_2.wald_test(formula=hypothesis) # type: ignore[arg-type]
print(f'Wald Test para os coeficientes de interesse (H0: β0 = β1 = β2 = β3 = 0)')
print(wald_test_1)

# TESTE 6 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0 bilateral
hypothesis_acumulado_bi = (
    'share_desembolso_real_pib_real_ano_anterior'
    ' + share_desembolso_real_pib_real_ano_anterior_lag1'
    ' + share_desembolso_real_pib_real_ano_anterior_lag2'
    ' + share_desembolso_real_pib_real_ano_anterior_lag3 = 0'
)

wald_test_2 = res_a1_2.wald_test(formula=hypothesis_acumulado_bi) # type: ignore[arg-type]
print(f'Wald Test para o efeito acumulado (H0: ∑(K=0--3) β(k) = 0 bilateral)')
print(wald_test_2)

# TESTE 7 - Teste de efeito acumulado H0​:∑(K=0--3)​β(k)​=0 unilateral
p_bilateral = float(wald_test_2.pval)

# nomes dos coeficientes que entram na soma
beta_names = [
    'share_desembolso_real_pib_real_ano_anterior',
    'share_desembolso_real_pib_real_ano_anterior_lag1',
    'share_desembolso_real_pib_real_ano_anterior_lag2',
    'share_desembolso_real_pib_real_ano_anterior_lag3',
]

# soma pontual (R*b)
beta_sum = float(res_a1_2.params[beta_names].sum())

# matriz de covariância dos estimadores
V = res_a1_2.cov  # pandas DataFrame (p x p)

# variância da soma: 1' V 1, restrita ao sub-bloco dos betas
V_sub = V.loc[beta_names, beta_names].to_numpy()
var_sum = float(np.ones(4) @ V_sub @ np.ones(4))
se_sum = float(np.sqrt(var_sum))

# t-stat do acumulado
t_sum = beta_sum / se_sum

# p-valor unilateral derivado do Wald bilateral (igualdade), usando sinal
p_bilateral = float(wald_test_2.pval)
if beta_sum > 0:
    p_unilateral = p_bilateral / 2
else:
    p_unilateral = 1 - (p_bilateral / 2)

# empacotar para salvar
wald_acumulado_t_uni = SimpleTest(stat=float(t_sum), pval=float(p_unilateral), df=1)

print("Acumulado:")
print("soma dos betas:", beta_sum)
print("se(soma):", se_sum)
print("t(soma):", t_sum)
print("p unilateral (H1: soma > 0):", p_unilateral)

salvar_resultados_panelols(res_a1_2, model_name="model_a1_2", out_dir=OUTPUTS_PATH, wald_tests={"wald_betas": wald_test_1, "wald_acumulado_bi": wald_test_2, "wald_acumulado_uni": wald_acumulado_t_uni}, overwrite=True,)
# %% ANÁLISE 3 - MODELO A2.1 BASELINE COM LEADS (PRETREND)
# MODELO A2.1 BASELINE - Evolução do PIB real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - FE 2-way (municípios e anos)
# EQUAÇÃO DO MODELO: ΔlogPIB(it​) = β0​X(it) ​+ β1​X(i,t−1) ​+ β2​X(i,t−2) ​+ β3​X(i,t−3) ​+ θ1​X(i,t+1)​ + θ2​X(i,t+2)​ + γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + γ4​logPOP(i,t−1​) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
# SE cluster (município + ano)
# Inseridos leads de 1 e 2 anos para teste de pretend
# Valores deflacionados pelo PIB - em termos reais de 2021
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Teste de pretend - Efeito Lead 1 e 2 anos dos desembolsos do BNDES no crescimento do PIB real
# H₀: θ1​ = θ2​​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB real no período 𝑡 e o desembolso do BNDES no período t+1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Selecionar dados de interesse para a regressão antes de dropNA (para manter o máximo de observações possível)
df_model_a2_1 = df_model[lhs_modelo_a2_1 + rhs_modelo_a2_1].copy().dropna()

# Variável dependente Y
y = df_model_a2_1[lhs_modelo_a2_1[0]]

# Variáveis independentes X (modelo principal)
X = df_model_a2_1[rhs_modelo_a2_1]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_a2_1 = mod.fit(
    cov_type='clustered',
    cluster_entity=True,
    cluster_time=True
)

hypothesis_leads_pib = [
    'share_desembolso_real_pib_real_ano_anterior_lead1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lead2 = 0',
]

wald_leads_pib = res_a2_1.wald_test(formula=hypothesis_leads_pib)  # type: ignore[arg-type]

print(f'Resumo do MODELO A2.1 BASELINE de regressão COM LEADS efeitos fixos duplos (municipais e de ano) - SE clusterizados por município e ano:')
print(res_a2_1.summary)

print('Wald Test para os leads (H0: θ1 = θ2 = 0)')
print(wald_leads_pib)

salvar_resultados_panelols(res_a2_1, model_name="model_a2_1", out_dir=OUTPUTS_PATH, wald_tests={"wald_leads": wald_leads_pib}, overwrite=True,)
# %% ANÁLISE 4 - MODELO A2.2 COMPARATIVO COM LEADS (PRETREND)
# MODELO A2.2 COMPARATIVO - ΔlogPIBpc(it​)
# EQUAÇÃO DO MODELO: ΔlogPIBpc(it​) = ∑(k=0--3)​βk​X(i,t−k) ​+ θ1​X(i,t+1)​ + θ2​X(i,t+2)​ + γ1​logPIBpc(i,t−1​) + γ2​share_industria(i,t−1) ​+ γ3​share_agropecuaria(i,t−1) + α(i) ​+ λ(t)​ + ε(it)​
# X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Teste de pretend - Efeito Lead 1 e 2 anos dos desembolsos do BNDES no crescimento do PIB per capita real
# H₀: θ1​ = θ2​​ = Zero
# H₀: Não há associação estatisticamente significativa entre o crescimento do PIB per capita real no período 𝑡 e o desembolso do BNDES no período t+1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Selecionar dados de interesse para a regressão antes de dropNA (para manter o máximo de observações possível)
df_model_a2_2 = df_model[lhs_modelo_a2_2 + rhs_modelo_a2_2].copy().dropna()

# Variável dependente Y
y = df_model_a2_2[lhs_modelo_a2_2[0]]

# Variáveis independentes X (modelo principal)
X = df_model_a2_2[rhs_modelo_a2_2]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_a2_2 = mod.fit(
    cov_type='clustered',
    cluster_entity=True,
    cluster_time=True
)

hypothesis_leads_pib = [
    'share_desembolso_real_pib_real_ano_anterior_lead1 = 0',
    'share_desembolso_real_pib_real_ano_anterior_lead2 = 0',
]

wald_leads_pibpc = res_a2_2.wald_test(formula=hypothesis_leads_pib)  # type: ignore[arg-type]

print(f'Resumo do MODELO A2.2 COMPARATIVO de regressão COM LEADS efeitos fixos duplos (municipais e de ano) - SE clusterizados por município e ano:')
print(res_a2_2.summary)

print('Wald Test para os leads (H0: θ1 = θ2 = 0)')
print(wald_leads_pibpc)

salvar_resultados_panelols(res_a2_2, model_name="model_a2_2", out_dir=OUTPUTS_PATH, wald_tests={"wald_leads": wald_leads_pibpc}, overwrite=True,)
# %% ANÁLISE 5 - MODELO B1.1 DE CONTRACICLICIDADE COM PIB REAL
# MODELO B1.1 DE CONTRACICLICIDADE COM PIB REAL - Evolução do PIB real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - FE 2-way (municípios e anos)
# EQUAÇÃO DO MODELO: Xit ​= δ0​gPIB(it)​+δ1​gPIB(i,t−1)​+δ2​gPIB(i,t−2)​+Φ′Zi,t−1​+αi​+λt​+uit​
# X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: δ0​ = 0
# H₀: O desembolso corrente não reage ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: δ1​ = 0
# H₀: O desembolso do ano anterior não reage negativamente ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: δ2​ = 0
# H₀: O desembolso de dois anos atrás não reage negativamente ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: δ1​ >= ZERO
# H₀: O desembolso não reage negativamente ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: O ciclo econômico influencia desembolsos.
# H₀: δ0​ = δ1​ = δ2​ = ZERO
# H₀: O ciclo econômico não influencia os desembolsos. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Selecionar dados de interesse para a regressão antes de dropNA (para manter o máximo de observações possível)
df_model_b1_1 = df_model[lhs_modelo_b1_1 + rhs_modelo_b1_1].copy()

# Drop NA
df_model_b1_1 = df_model_b1_1.dropna()

# Variável dependente Y
y = df_model_b1_1[lhs_modelo_b1_1[0]]

# Variáveis independentes X (modelo principal)
X = df_model_b1_1[rhs_modelo_b1_1]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_b1_1 = mod.fit(
    cov_type='clustered',
    cluster_entity=True,
    cluster_time=True
)

print(f'Resumo do MODELO B1.1 DE CONTRACICLICIDADE (PIB REAL) de regressão com efeitos fixos duplos (municipais e de ano) - SE clusterizados por município e ano:')
print(res_b1_1.summary)

# TESTE 1 - δ1​ >= ZERO
coef_name = 'delta_log_pib_real_lag1'
delta1_hat = float(res_b1_1.params[coef_name])
t_delta1 = float(res_b1_1.tstats[coef_name])

# p-valor bilateral do output padrão
p_bi_delta1 = float(res_b1_1.pvalues[coef_name])

# converter para unilateral à esquerda
# Se t é negativo (efeito contracíclico), p_uni = p_bi/2
# Se t é positivo, p_uni = 1 - p_bi/2
if t_delta1 < 0:
    p_uni_delta1 = p_bi_delta1 / 2
else:
    p_uni_delta1 = 1 - (p_bi_delta1 / 2)

teste_delta1_uni = SimpleTest(stat=t_delta1, pval=p_uni_delta1, df=1)

print('Teste unilateral contraciclicidade (H0: δ1 >= 0)')
print('delta1_hat:', delta1_hat)
print('t(delta1):', t_delta1)
print('p unilateral:', p_uni_delta1)

# -------------------------
# TESTE 2: Wald conjunto do ciclo H0: δ0 = δ1 =δ2 =0 (bilateral)
hyp_ciclo = [
    'delta_log_pib_real = 0',
    'delta_log_pib_real_lag1 = 0',
    'delta_log_pib_real_lag2 = 0',
]
wald_ciclo = res_b1_1.wald_test(formula = hyp_ciclo)  # type: ignore[arg-type]

print('Wald (ciclo) H0: δ0 = δ1 = δ2 = 0')
print(wald_ciclo)

# Salvar
salvar_resultados_panelols(res_b1_1, model_name="modelb1_1", out_dir=OUTPUTS_PATH, wald_tests={"delta1_uni": teste_delta1_uni, "wald_ciclo": wald_ciclo}, overwrite=True)
# %% ANÁLISE 6 - MODELO B2.1 DE CONTRACICLICIDADE SETORIAL COM PIB REAL
# MODELO B2.1 DE CONTRACICLICIDADE SETORIAL COM PIB REAL - Evolução do PIB real ao longo do tempo em respeito aos desembolsos do BNDES para industria e para o agronegócio para cada município (efeito regional) - FE 2-way (municípios e anos)
# EQUAÇÃO DO MODELO: Xind(it)​ = δ0ind​gind(it)​ + δ1ind​gind(i,t−1) +δ2ind​gind(i,t−2)​ + Φind′Z(i,t−1) ​+αi ​+ λt ​+ uind(it)
# E
# EQUAÇÃO DO MODELO: Xagro(it)​ = δ0agro​gagro(it)​ + δ1agro​gagro(i,t−1) +δ2agro​gagro(i,t−2)​ + Φagro′Z(i,t−1) ​+αi ​+ λt ​+ uagro(it)
# X = (Desembolso do BNDES medido como proporção do PIB do período anterior)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: δ0​ = 0
# H₀: O desembolso corrente não reage ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: δ1​ = 0
# H₀: O desembolso do ano anterior não reage negativamente ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: δ2​ = 0
# H₀: O desembolso de dois anos atrás não reage negativamente ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: δ1​ >= ZERO
# H₀: O desembolso não reage negativamente ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: O ciclo econômico influencia desembolsos.
# H₀: δ0​ = δ1​ = δ2​ = ZERO
# H₀: O ciclo econômico não influencia os desembolsos. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

# Selecionar dados de interesse para a regressão antes de dropNA (para manter o máximo de observações possível)
df_model_b2_1_ind = df_model[lhs_modelo_b2_1_ind + rhs_modelo_b2_1_ind].copy()

# Drop NA
df_model_b2_1_ind = df_model_b2_1_ind.dropna()

# Variável dependente Y
y = df_model_b2_1_ind[lhs_modelo_b2_1_ind[0]]

# Variáveis independentes X (modelo principal)
X = df_model_b2_1_ind[rhs_modelo_b2_1_ind]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_b2_1_ind = mod.fit(
    cov_type='clustered',
    cluster_entity=True,
    cluster_time=True
)

print(f'Resumo do MODELO B2.1 DE CONTRACICLICIDADE (PIB REAL) SETOR INDÚSTRIA de regressão com efeitos fixos duplos (municipais e de ano) - SE clusterizados por município e ano:')
print(res_b2_1_ind.summary)

# TESTE 1 - δ1​ >= ZERO
coef_name = 'delta_asinh_va_industria_real_lag1'
delta1_hat = float(res_b2_1_ind.params[coef_name])
t_delta1 = float(res_b2_1_ind.tstats[coef_name])

# p-valor bilateral do output padrão
p_bi_delta1 = float(res_b2_1_ind.pvalues[coef_name])

# converter para unilateral à esquerda
# Se t é negativo (efeito contracíclico), p_uni = p_bi/2
# Se t é positivo, p_uni = 1 - p_bi/2
if t_delta1 < 0:
    p_uni_delta1 = p_bi_delta1 / 2
else:
    p_uni_delta1 = 1 - (p_bi_delta1 / 2)

teste_delta1_uni = SimpleTest(stat=t_delta1, pval=p_uni_delta1, df=1)

print('Teste unilateral contraciclicidade (H0: δ1 >= 0)')
print('delta1_hat:', delta1_hat)
print('t(delta1):', t_delta1)
print('p unilateral:', p_uni_delta1)

# -------------------------
# TESTE 2: Wald conjunto do ciclo H0: δ0 = δ1 =δ2 =0 (bilateral)
hyp_ciclo = [
    'delta_asinh_va_industria_real = 0',
    'delta_asinh_va_industria_real_lag1 = 0',
    'delta_asinh_va_industria_real_lag2 = 0',
]

wald_ciclo = res_b2_1_ind.wald_test(formula = hyp_ciclo)  # type: ignore[arg-type]

print('Wald (ciclo) H0: δ0 = δ1 = δ2 = 0')
print(wald_ciclo)

# Salvar
salvar_resultados_panelols(res_b2_1_ind, model_name="modelb2_1_ind", out_dir=OUTPUTS_PATH, wald_tests={"delta1_uni": teste_delta1_uni, "wald_ciclo": wald_ciclo}, overwrite=True)

# Selecionar dados de interesse para a regressão antes de dropNA (para manter o máximo de observações possível)
df_model_b2_1_agro = df_model[lhs_modelo_b2_1_agro + rhs_modelo_b2_1_agro].copy()

# Drop NA
df_model_b2_1_agro = df_model_b2_1_agro.dropna()

# Variável dependente Y
y = df_model_b2_1_agro[lhs_modelo_b2_1_agro[0]]

# Variáveis independentes X (modelo principal)
X = df_model_b2_1_agro[rhs_modelo_b2_1_agro]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_b2_1_agro = mod.fit(
    cov_type='clustered',
    cluster_entity=True,
    cluster_time=True
)

print(f'Resumo do MODELO B2.1 DE CONTRACICLICIDADE (PIB REAL) SETOR AGROPECUÁRIO de regressão com efeitos fixos duplos (municipais e de ano) - SE clusterizados por município e ano:')
print(res_b2_1_agro.summary)

# TESTE 1 - δ1​ >= ZERO
coef_name = 'delta_asinh_va_agropecuaria_real_lag1'
delta1_hat = float(res_b2_1_agro.params[coef_name])
t_delta1 = float(res_b2_1_agro.tstats[coef_name])

# p-valor bilateral do output padrão
p_bi_delta1 = float(res_b2_1_agro.pvalues[coef_name])

# converter para unilateral à esquerda
# Se t é negativo (efeito contracíclico), p_uni = p_bi/2
# Se t é positivo, p_uni = 1 - p_bi/2
if t_delta1 < 0:
    p_uni_delta1 = p_bi_delta1 / 2
else:
    p_uni_delta1 = 1 - (p_bi_delta1 / 2)

teste_delta1_uni = SimpleTest(stat=t_delta1, pval=p_uni_delta1, df=1)

print('Teste unilateral contraciclicidade (H0: δ1 >= 0)')
print('delta1_hat:', delta1_hat)
print('t(delta1):', t_delta1)
print('p unilateral:', p_uni_delta1)

# -------------------------
# TESTE 2: Wald conjunto do ciclo H0: δ0 = δ1 =δ2 =0 (bilateral)
hyp_ciclo = [
    'delta_asinh_va_agropecuaria_real = 0',
    'delta_asinh_va_agropecuaria_real_lag1 = 0',
    'delta_asinh_va_agropecuaria_real_lag2 = 0',
]
wald_ciclo = res_b2_1_agro.wald_test(formula = hyp_ciclo)  # type: ignore[arg-type]

print('Wald (ciclo) H0: δ0 = δ1 = δ2 = 0')
print(wald_ciclo)

# Salvar
salvar_resultados_panelols(res_b2_1_agro, model_name="modelb2_1_agro", out_dir=OUTPUTS_PATH, wald_tests={"delta1_uni": teste_delta1_uni, "wald_ciclo": wald_ciclo}, overwrite=True)
# %% ANÁLISE 7 - MODELO B3 PROBABILIDADE DE RECEBER DESBOLSO - FE 2-way (municípios e anos) (Linear Probability Model)
# MODELO B3 PROBABILIDADE DE DESEMBOLSO - Efeito do crescimento na probabilidade de receber desembolso - FE 2-way (municípios e anos)
# EQUAÇÃO DO MODELO: Dit ​= κ0​gPIB(i,t)​ + κ1​gPIB(i,t-1)​ + κ2​gPIB(i,t-2) ​+ Ω′Z(i,t−1) ​+ αi ​+λt ​+ eit​
# Dit = 1[Desembolsoit>0]
# SE cluster (município + ano)
# Valores deflacionados pelo PIB - em termos reais de 2021
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 1: Crescimento contemporâneo não afeta probabilidade de desembolso
# H₀: k0​ = Zero
# H₀: Não há associação estatisticamente significativa entre a probabilidade de receber desembolso do BNDES no período t e o crescimento do PIB real no mesmo período, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 2: Efeito defasado de um ano do crescimento na probabilidade de receber desembolso
# H₀: k1​ = Zero
# H₀: Não há associação estatisticamente significativa entre a probabilidade de receber desembolso do BNDES no período t e o crescimento do PIB real no período t−1, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 3: Efeito defasado de dois anos do crescimento na probabilidade de receber desembolso
# H₀: k2​ = Zero
# H₀: Não há associação estatisticamente significativa entre a probabilidade de receber desembolso do BNDES no período t e o crescimento do PIB real no período t−2, controlando por efeitos fixos municipais e efeitos fixos de ano.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 4: O desembolso aumenta após desaceleração econômica local. Evidência compatível com atuação contracíclica.
# H₀: k1​ >= ZERO
# H₀: O desembolso não reage negativamente ao crescimento passado. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
# TESTE DE HIPÓTESE 5: O ciclo econômico influencia desembolsos.
# H₀: k0​ = k1​ = k2​ = ZERO
# H₀: O ciclo econômico não influencia os desembolsos. Não há evidência de atuação contracíclica.
#----------------------------------------------------------------------------------------------------------------------------------

# Selecionar dados de interesse para a regressão antes de dropNA (para manter o máximo de observações possível)
df_model_b3 = df_model[lhs_modelo_b3 + rhs_modelo_b3].copy().dropna()

df_model_b3["D_recebeu_desembolso"] = (df_model_b3["desembolsos_corrente"] > 0).astype(float)

lhs_model_b3 = ["D_recebeu_desembolso"]

# Variável dependente Y
y = df_model_b3[lhs_modelo_b3[0]]

# Variáveis independentes X (modelo principal)
X = df_model_b3[rhs_modelo_b3]

# Rodar modelo com FE duplo
mod = PanelOLS(
    y,
    X,
    entity_effects=True,
    time_effects=True
)

res_b3 = mod.fit(
    cov_type='clustered',
    cluster_entity=True,
    cluster_time=True
)

print(f'Resumo do MODELO B3 Linear Probability Model - regressão com efeitos fixos duplos (municipais e de ano) - SE clusterizados por município e ano:')
print(res_b3.summary)

# TESTE 4: Unilateral contraciclo (principal) H0: k1 >= 0 vs H1: k1 < 0
coef_name = "delta_log_pib_real_lag1"
k1_hat = float(res_b3.params[coef_name])
t_k1 = float(res_b3.tstats[coef_name])
p_bi_k1 = float(res_b3.pvalues[coef_name])

if t_k1 < 0:
    p_uni_k1 = p_bi_k1 / 2
else:
    p_uni_k1 = 1 - (p_bi_k1 / 2)

teste_k1_uni = SimpleTest(stat=t_k1, pval=p_uni_k1, df=1)

print("Teste unilateral contraciclicidade (B3): H0 k1>=0 vs H1 k1<0")
print("k1_hat:", k1_hat, " t:", t_k1, " p_uni:", p_uni_k1)

# TESTE 5 - Wald Test para os coeficientes de interesse H0​: k0 ​= k1 ​= k2 ​= 0
hyp_ciclo = [
    "delta_log_pib_real = 0",
    "delta_log_pib_real_lag1 = 0",
    "delta_log_pib_real_lag2 = 0",
]
wald_ciclo = res_b3.wald_test(formula=hyp_ciclo)  # type: ignore[arg-type]
print("Wald conjunto (ciclo) H0: k0 = k1 = k2 = 0")
print(wald_ciclo)

salvar_resultados_panelols(res_b3, model_name="model_b3", out_dir=OUTPUTS_PATH, wald_tests={"wald_ciclo": wald_ciclo, "k1_uni": teste_k1_uni}, overwrite=True,)

print(df_model.info())