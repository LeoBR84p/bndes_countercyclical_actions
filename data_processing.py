# %% IMPORTAÇÕES INICIAIS
# IMPORTAÇÃO DE BIBLIOTECAS E PATHS
# Importando as bibliotecas necessárias
import pandas as pd
import numpy as np
import unicodedata
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import gc

# Importanto variáveis de paths
# ! Importante que os diretórios já existam, caso contrário: execute paths.py.
from paths import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH
# %% DADOS DE POPULAÇÃO DOS MUNICÍPIOS BRASILEIROS
# INGESTÃO DE DADOS 1 DE 4 - POPULAÇÃO DOS MUNICÍPIOS BRASILEIROS - DATASUS

# Fonte 1: DATASUS do Ministério da Saúde - População residente por município - Brasil
# http://tabnet.datasus.gov.br/cgi/tabcgi.exe?ibge/cnv/popsvs2024br.def

# Importar o arquivo CSV e renomear a coluna 'Município' para 'Municipio_original'
# Separar a coluna 'Municipio_original' em duas novas colunas: 'Código' e 'Município'
# Fixar tipagem das colunas 'Código' e 'Município' para texto (string)
df_hab = pd.read_csv(Path(RAW_DATA_PATH) / 'POP_MUNICIPIOS.csv', encoding='utf-8', sep=';')
df_hab.rename(columns={'Município': 'municipio_original'}, inplace=True)
df_hab[['codigo', 'municipio']] = df_hab['municipio_original'].str.extract(r'^(\d{1,6})\s+(.*)$')
cols = ['codigo', 'municipio'] + [col for col in df_hab.columns if col not in ['codigo', 'municipio', 'municipio_original']]
df_hab = df_hab[cols]
df_hab['codigo'] = df_hab['codigo'].astype('string')
df_hab['municipio'] = df_hab['municipio'].astype('string')

# TODO Verificações opcionais
"""
print(f'Tipagens das colunas do DataFrame de população do Datasus:')
print(df_hab.dtypes)
print(f'Número de linhas e colunas: {df_hab.shape}')
print(f'Exemplo de dados:')
print(df_hab.head())
print(f'Número de dados vazios: {np.count_nonzero(df_hab.isna().to_numpy())}')
print(f'Linhas com dados vazios: {np.count_nonzero(df_hab.isna().to_numpy().any(axis=1))}')
print(f'Conteúdo das linhas com dados vazios:')
print(df_hab[df_hab.isna().to_numpy().any(axis=1)])
"""

# Drop linhas vazias
df_hab.dropna(inplace=True)

# TODO Verificações opcionais
"""
print(f'Número de linhas e colunas após drop: {df_hab.shape}')
print(f'Número de dados vazios após drop: {np.count_nonzero(df_hab.isna().to_numpy())}')
print(f'Linhas com dados vazios após drop: {np.count_nonzero(df_hab.isna().to_numpy().any(axis=1))}')
print(f'Conteúdo das linhas com dados vazios após drop:')
print(df_hab[df_hab.isna().to_numpy().any(axis=1)])
"""

# ! DataFrame não possui informações sobre os Estados associados, sendo aconselhado incluir de maneira objetiva
# Mapa oficial de codigos IBGE (UF) - https://www.ibge.gov.br/explica/codigos-dos-municipios.php
uf_map = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL",
    28: "SE", 29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR",
    42: "SC", 43: "RS", 50: "MS", 51: "MT", 52: "GO", 53: "DF"
}

# Incluir nova coluna 'Estado' com base no código do município
df_hab['estado'] = df_hab['codigo'].str[:2].astype(int).map(uf_map)

print(f'Número de Estados únicos (correto = 27): {df_hab["estado"].nunique()}')
print(f'Total de Municípios-Estado únicos (correto = 5570 IBGE|2023): {df_hab[["codigo", "estado"]].drop_duplicates().shape[0]}')
print(f'Municípios com Estado vazio (correto = 0): {df_hab[df_hab["estado"].isna()]["codigo"].nunique()}')
print(f'Número de linhas e colunas: {df_hab.shape}')
print(f'Tipagens das colunas:\n{df_hab.dtypes}')

# Liberação de memória
del uf_map, cols #df_hab é utilizada posteriormente
gc.collect()

#_ ## CONCLUSÃO SOBRE A QUALIDADE DOS DADOS DE POPULAÇÃO DO DATASUS ###
#_ Após verificação, não há dados vazios no DataFrame de população do Datasus, o que é um bom sinal para a qualidade dos dados. 
#_ O número de municípios únicos é consistente com o número total de linhas, indicando que cada linha representa um município diferente. 
#_ As tipagens das colunas foram fixadas com 'Código' e 'Município' como strings, o que facilita a manipulação dos dados posteriormente.
#_ O exemplo de dados mostra que as informações estão organizadas com os códigos e nomes dos municípios seguidos pelas colunas de população.
#_ Não foram identificadas informações sobre os Estados associados aos Municípios na base original. Por isso, foi realizada a inserção dessa informação para uso posterior.
#_ As linhas vazias foram verificadas e não continham dados relevantes (somatórios ou totais), o que confirma que não há dados faltantes significativos no DataFrame.
#_ Total de municípios únicos: 5570 equivale ao total divulgado pelo IBGE, o que é um bom indicativo de qualidade dos dados.
#_ Esta base do IBGE considera convenção antiga de Código de Município com 6 dígitos (sem o digito verificador).
#_ https://www.ibge.gov.br/pt/cidades-e-estados.html?lang=pt-BR
#_ ##---------------------------------------------------------------###
# %% DADOS DE DEFLATORES PARA PIB E DESEMBOLSOS TOTAIS
# INGESTÃO DE DADOS 2 DE 4 -  DEFLATORES PARA PIB E DESEMBOLSOS TOTAIS - IBGE
# Fonte 2: Tabela 6 - Produto Interno Bruto, Produto Interno Bruto per capita, população residente e deflator - 1996-2023
# https://ftp.ibge.gov.br/Contas_Nacionais/Sistema_de_Contas_Nacionais/2023/tabelas_xls/sinoticas/tab06.xls

# Importar o arquivo Excel
df_deflator_pib = pd.read_excel(Path(RAW_DATA_PATH) / 'tab06_deflator_pib.xlsx', skiprows=5)

# Renomear Unnamed: 0 para ano, Valores\ncorrentes para pib_corrente, Preços do\nano anterior para pib_constante, drop demais colunas
df_deflator_pib.rename(columns={'Unnamed: 0': 'ano', 'Valores\ncorrentes': 'pib_corrente', 'Preços do\nano anterior': 'pib_constante'}, inplace=True)
df_deflator_pib = df_deflator_pib[['ano', 'pib_corrente', 'pib_constante']]

# Converter coluna ano , pib_corrente e pib_constante para float, tratando vírgula como separador decimal
for col in ['ano', 'pib_corrente', 'pib_constante']:
    df_deflator_pib[col] = pd.to_numeric(df_deflator_pib[col], errors='coerce')

# Selecionar dados entre 2002 e 2023
df_deflator_pib = df_deflator_pib[(df_deflator_pib['ano'] >= 2002) & (df_deflator_pib['ano'] <= 2023)].copy()

# Calcular a variação anual do deflator (PIB_corrente / PIB_constante)
# Este valor representa quanto o PIB corrente cresceu em relação ao PIB a preços do ano anterior
df_deflator_pib['variacao_deflator'] = (df_deflator_pib['pib_corrente'] / df_deflator_pib['pib_constante']).astype('float64')

# Ordenar por ano para garantir ordem cronológica crescente
df_deflator_pib = df_deflator_pib.sort_values('ano').reset_index(drop=True)

# Calcular índice encadeado acumulado com base em 2021 = 100
df_deflator_pib['indice_encadeado'] = 100.0

# Encontrar o índice do ano 2021
idx_2021 = df_deflator_pib[df_deflator_pib['ano'] == 2021].index
if len(idx_2021) > 0:
    idx_2021 = idx_2021[0]
    
    # Iterar de trás para frente (de 2020 até 2002)
    for i in range(idx_2021 - 1, -1, -1):
        # D_{t+1} = variacao_deflator do ano seguinte (inflação implícita de t -> t+1)
        variacao = df_deflator_pib.at[i + 1, 'variacao_deflator']
        indice_seguinte = df_deflator_pib.at[i + 1, 'indice_encadeado']
        df_deflator_pib.at[i, 'indice_encadeado'] = indice_seguinte / variacao #type: ignore
    
    # Iterar para frente (de 2022 em diante)
    for i in range(idx_2021 + 1, len(df_deflator_pib)):
        variacao = df_deflator_pib.at[i - 1, 'variacao_deflator']
        indice_anterior = df_deflator_pib.at[i - 1, 'indice_encadeado']
        df_deflator_pib.at[i, 'indice_encadeado'] = indice_anterior * variacao #type: ignore

# Formato final, apenas colunas Ano e indice_encadeado (renomear para deflator_pib_2021)
df_deflator_pib = df_deflator_pib[['ano', 'indice_encadeado']]
df_deflator_pib.rename(columns={'indice_encadeado': 'deflator_pib_2021'}, inplace=True)

# TODO Verificações opcionais
"""
print(f'\nDataFrame de deflator do PIB:')
print(f'Tipagens das colunas: {df_deflator_pib.dtypes}')
print(f'Número de linhas e colunas: {df_deflator_pib.shape}')
print(f'Dados:')
print(df_deflator_pib)
"""

# APURAÇÃO DE DEFLATORES PARA PIB INDUSTRIAL + DESEMBOLSOS INDUSTRIAIS E PIB AGROPECUÁRIA + DESEMBOLSOS AGROPECUÁRIA
# Fonte 3: Tabela 10.1 - Valor adicionado bruto constante e corrente, segundo os grupos de atividades - 2000-2023
# https://ftp.ibge.gov.br/Contas_Nacionais/Sistema_de_Contas_Nacionais/2023/tabelas_xls/sinoticas/tab10_1.xls

# Importar o arquivo Excel
df_deflator_pib_industria = pd.read_excel(Path(RAW_DATA_PATH) / 'tab10_1_deflator_pib_setor.xlsx', skiprows=4)

# Renomear Unnamed: 1 para setor, manter dados apenas de setor 'Indústria', drop demais linhas
df_deflator_pib_industria.rename(columns={'Unnamed: 1': 'setor'}, inplace=True)
df_deflator_pib_industria = df_deflator_pib_industria[df_deflator_pib_industria['setor'] == 'Indústria'].copy()

# Drop e renomear outras colunas
# Converter colunas para string para garantir compatibilidade
df_deflator_pib_industria.columns = df_deflator_pib_industria.columns.astype(str)

# Criar dicionário de mapeamento dinâmico para anos 2000-2023
rename_dict = {'2000': '2000_corrente'}

for ano in range(2001, 2022):
    unnamed_col = 2 * (ano - 1999)
    rename_dict[str(ano)] = f'{ano}_constante'
    rename_dict[f'Unnamed: {unnamed_col}'] = f'{ano}_corrente'

df_deflator_pib_industria.rename(columns=rename_dict, inplace=True)

# Criar deflator_pib_industria_2021 utilizando a fórmula: deflator = PIB_corrente / PIB_constante em formato de tabela LONG (Ano, deflator_pib_industria_2021)
deflator_data = []
for ano in range(2001, 2024):
    corrente_col = f'{ano}_corrente'
    constante_col = f'{ano}_constante'
    if corrente_col in df_deflator_pib_industria.columns and constante_col in df_deflator_pib_industria.columns:
        corrente = df_deflator_pib_industria[corrente_col].values[0]
        constante = df_deflator_pib_industria[constante_col].values[0]
        if pd.notna(corrente) and pd.notna(constante) and constante != 0:
            variacao = corrente / constante
            deflator_data.append({'ano': ano, 'variacao_deflator': variacao})

df_deflator_pib_industria_processado = pd.DataFrame(deflator_data)
df_deflator_pib_industria_processado = df_deflator_pib_industria_processado.sort_values('ano').reset_index(drop=True)

# Calcular índice encadeado acumulado com base em 2021 = 100
df_deflator_pib_industria_processado['indice_encadeado'] = 100.0

# Encontrar o índice do ano 2021
idx_2021 = df_deflator_pib_industria_processado[df_deflator_pib_industria_processado['ano'] == 2021].index
if len(idx_2021) > 0:
    idx_2021 = idx_2021[0]
    
    # Iterar de trás para frente (de 2020 até 2001)
    for i in range(idx_2021 - 1, -1, -1):
        variacao = df_deflator_pib_industria_processado.at[i + 1, 'variacao_deflator']
        indice_seguinte = df_deflator_pib_industria_processado.at[i + 1, 'indice_encadeado']
        df_deflator_pib_industria_processado.at[i, 'indice_encadeado'] = indice_seguinte / variacao #type: ignore
    
    # Iterar para frente (de 2022 em diante)
    for i in range(idx_2021 + 1, len(df_deflator_pib_industria_processado)):
        variacao = df_deflator_pib_industria_processado.at[i - 1, 'variacao_deflator']
        indice_anterior = df_deflator_pib_industria_processado.at[i - 1, 'indice_encadeado']
        df_deflator_pib_industria_processado.at[i, 'indice_encadeado'] = indice_anterior * variacao #type: ignore

# Formato final, apenas colunas ano e indice_encadeado (renomear para deflator_pib_industria_2021)
df_deflator_pib_industria = df_deflator_pib_industria_processado[['ano', 'indice_encadeado']].copy()
df_deflator_pib_industria.rename(columns={'indice_encadeado': 'deflator_pib_industria_2021'}, inplace=True)

# Selecionar apenas anos entre 2002 e 2023
df_deflator_pib_industria = df_deflator_pib_industria[(df_deflator_pib_industria['ano'] >= 2002) & (df_deflator_pib_industria['ano'] <= 2023)].copy()

# TODO Verificações opcionais
"""
print(f'\nDataFrame de deflator do PIB industrial:')
print(f'Tipagens das colunas: {df_deflator_pib_industria.dtypes}')
print(f'Número de linhas e colunas: {df_deflator_pib_industria.shape}')
print(f'Exemplo de dados:')
print(df_deflator_pib_industria)
"""

#! REFAZER O PROCESSO PARA AGROPECUÁRIA
# Importar o arquivo Excel
df_deflator_pib_agropecuaria = pd.read_excel(Path(RAW_DATA_PATH) / 'tab10_1_deflator_pib_setor.xlsx', skiprows=4)

# Renomear Unnamed: 1 para setor, manter dados apenas de setor 'Agropecuária', drop demais linhas
df_deflator_pib_agropecuaria.rename(columns={'Unnamed: 1': 'setor'}, inplace=True)
df_deflator_pib_agropecuaria = df_deflator_pib_agropecuaria[df_deflator_pib_agropecuaria['setor'] == 'Agropecuária'].copy()

# Drop e renomear outras colunas
# Converter colunas para string para garantir compatibilidade
df_deflator_pib_agropecuaria.columns = df_deflator_pib_agropecuaria.columns.astype(str)

# Criar dicionário de mapeamento dinâmico para anos 2000-2023
rename_dict = {'2000': '2000_corrente'}

for ano in range(2001, 2022):
    unnamed_col = 2 * (ano - 1999)
    rename_dict[str(ano)] = f'{ano}_constante'
    rename_dict[f'Unnamed: {unnamed_col}'] = f'{ano}_corrente'

df_deflator_pib_agropecuaria.rename(columns=rename_dict, inplace=True)

# Criar deflator_pib_agropecuaria_2021 utilizando a fórmula: deflator = PIB_corrente / PIB_constante em formato de tabela LONG (Ano, deflator_pib_agropecuaria_2021)
deflator_data = []
for ano in range(2001, 2024):
    corrente_col = f'{ano}_corrente'
    constante_col = f'{ano}_constante'
    if corrente_col in df_deflator_pib_agropecuaria.columns and constante_col in df_deflator_pib_agropecuaria.columns:
        corrente = df_deflator_pib_agropecuaria[corrente_col].values[0]
        constante = df_deflator_pib_agropecuaria[constante_col].values[0]
        if pd.notna(corrente) and pd.notna(constante) and constante != 0:
            variacao = corrente / constante
            deflator_data.append({'ano': ano, 'variacao_deflator': variacao})

df_deflator_pib_agropecuaria_processado = pd.DataFrame(deflator_data)
df_deflator_pib_agropecuaria_processado = df_deflator_pib_agropecuaria_processado.sort_values('ano').reset_index(drop=True)

# Calcular índice encadeado acumulado com base em 2021 = 100
df_deflator_pib_agropecuaria_processado['indice_encadeado'] = 100.0

# Encontrar o índice do ano 2021
idx_2021 = df_deflator_pib_agropecuaria_processado[df_deflator_pib_agropecuaria_processado['ano'] == 2021].index
if len(idx_2021) > 0:
    idx_2021 = idx_2021[0]
    
    # Iterar de trás para frente (de 2020 até 2001)
    for i in range(idx_2021 - 1, -1, -1):
        variacao = df_deflator_pib_agropecuaria_processado.at[i + 1, 'variacao_deflator']
        indice_seguinte = df_deflator_pib_agropecuaria_processado.at[i + 1, 'indice_encadeado']
        df_deflator_pib_agropecuaria_processado.at[i, 'indice_encadeado'] = indice_seguinte / variacao #type: ignore
    
    # Iterar para frente (de 2022 em diante)
    for i in range(idx_2021 + 1, len(df_deflator_pib_agropecuaria_processado)):
        variacao = df_deflator_pib_agropecuaria_processado.at[i - 1, 'variacao_deflator']
        indice_anterior = df_deflator_pib_agropecuaria_processado.at[i - 1, 'indice_encadeado']
        df_deflator_pib_agropecuaria_processado.at[i, 'indice_encadeado'] = indice_anterior * variacao #type: ignore

# Formato final, apenas colunas ano e indice_encadeado (renomear para deflator_pib_agropecuaria_2021)
df_deflator_pib_agropecuaria = df_deflator_pib_agropecuaria_processado[['ano', 'indice_encadeado']].copy()
df_deflator_pib_agropecuaria.rename(columns={'indice_encadeado': 'deflator_pib_agropecuaria_2021'}, inplace=True)

# Selecionar apenas anos entre 2002 e 2023
df_deflator_pib_agropecuaria = df_deflator_pib_agropecuaria[(df_deflator_pib_agropecuaria['ano'] >= 2002) & (df_deflator_pib_agropecuaria['ano'] <= 2023)].copy()

# TODO Verificações opcionais
"""
print(f'\nDataFrame de deflator do PIB agropecuária:')
print(f'Tipagens das colunas: {df_deflator_pib_agropecuaria.dtypes}')
print(f'Número de linhas e colunas: {df_deflator_pib_agropecuaria.shape}')
print(f'Exemplo de dados:')
print(df_deflator_pib_agropecuaria)
"""

# ! UNIR TABELA DE DEFLATORES
# Realizar junção entre os DataFrames de deflatores utilizando a coluna Ano
# Usar como base todos os anos de deflator_pib_2021 e preencher com NaN deflator_pib_industria_2021 e deflator_pib_agropecuaria_2021 quando não presente
df_deflatores = pd.merge(
    df_deflator_pib,
    df_deflator_pib_industria,
    on='ano',
    how='left'
)
df_deflatores = pd.merge(
    df_deflatores,
    df_deflator_pib_agropecuaria,
    on='ano',
    how='left'
)
print(f'DataFrame unificado de deflatores:')
print(f'Número de linhas e colunas: {df_deflatores.shape}')
print(f'Tipagens das colunas:\n{df_deflatores.dtypes}')
print(f'Dados:')
print(df_deflatores)

# Salvar tabela de deflatores unificada em formato Parquet com pyarrow
tabela_deflatores = pa.Table.from_pandas(df_deflatores)
pq.write_table(tabela_deflatores, Path(PROCESSED_DATA_PATH) / 'tabela_deflatores.parquet')

# Liberação de memória
del idx_2021, df_deflator_pib, df_deflator_pib_industria, df_deflator_pib_industria_processado, df_deflator_pib_agropecuaria, df_deflator_pib_agropecuaria_processado, df_deflatores, tabela_deflatores
gc.collect()

#_ ## CONCLUSÃO SOBRE A QUALIDADE DOS DADOS DE DEFLATORES PIB ###
#_ Após verificação, não há dados vazios no DataFrame de deflatores do PIB, o que é um bom sinal para a qualidade dos dados. 
#_ O número de anos únicos é consistente com o número total de linhas, indicando que cada linha representa um ano diferente. 
#_ Existem informações de pib_industria apenas até 2021.
#_ O exemplo de dados mostra que as informações estão organizadas com os anos seguidos pelas colunas de deflatores.
#_ Deflatores prontos para uso, onde 2021 = 100 e os anos anteriores/posteriores representam o valor relativo em relação a 2021.
#_##---------------------------------------------------------###
# %% DADOS DE PIB CORRENTE E VALOR ADICIONADO DOS MUNICÍPIOS BRASILEIROS
# INGESTÃO DE DADOS 3 DE 4 - PIB CORRENTE E VALOR ADICIONADO DOS MUNICÍPIOS BRASILEIROS - IBGE
# Fonte 4: Dados do PIB per capita dos municípios brasileiros - IBGE
# Tabela 5938 - Produto interno bruto a preços correntes, impostos, líquidos de subsídios, sobre produtos a preços correntes e valor adicionado bruto a preços correntes total e por atividade econômica, e respectivas participações - Referência 2010
# https://sidra.ibge.gov.br/tabela/5938

# Importar o arquivo CSV do PIB per capita
# Fixar tipagem de Ano e das demais colunas como numéricas, tratando vírgula como separador decimal
# Renomear coluna 'Brasil, Grande Região, Unidade da Federação e Município' para Local
# Fixar tipagem de Local como string
df_pib = pd.read_csv(Path(RAW_DATA_PATH) / 'PIB2002-2023.csv', encoding='utf-8', sep=';', skiprows=2, low_memory=False)

# Drop linhas com Local vazio e onde todas as colunas numéricas estão vazias, indicando que não há dados relevantes (somatórios ou totais)
df_pib.rename(columns={'Brasil, Grande Região, Unidade da Federação e Município': 'local'}, inplace=True)
df_pib['local'] = df_pib['local'].astype('string')
df_pib.dropna(subset=['local'], inplace=True)
df_pib.dropna(subset=df_pib.columns[2:], how='all', inplace=True)

# Converter colunas numéricas
for col in df_pib.columns[2:]:
    df_pib[col] = pd.to_numeric(df_pib[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce')

# Ajustar Ano para ano numérico
df_pib['ano'] = pd.to_numeric(df_pib['Ano'], errors='coerce')
df_pib.drop(columns=['Ano'], inplace=True)

# Renomear colunas para pib_corrente, impostos_corrente, va_total_corrente, va_agropecuaria_corrente, va_industria_corrente, va_servicos_corrente, va_administracao_corrente
df_pib.rename(columns=
    {
    'Produto Interno Bruto a preços correntes (Mil Reais)': 'pib_corrente',
    'Impostos, líquidos de subsídios, sobre produtos a preços correntes (Mil Reais)': 'impostos_corrente',
    'Valor adicionado bruto a preços correntes total (Mil Reais)': 'va_total_corrente',
    'Valor adicionado bruto a preços correntes da agropecuária (Mil Reais)': 'va_agropecuaria_corrente',
    'Valor adicionado bruto a preços correntes da indústria (Mil Reais)': 'va_industria_corrente',
    'Valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social (Mil Reais)': 'va_servicos_corrente',
    'Valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social (Mil Reais)': 'va_administracao_corrente'
    }, inplace=True)

# TODO Verificações opcionais
"""
print(f'Tipagens das colunas do DataFrame de PIB do IBGE:')
print(df_pib.dtypes)
print(f'Número de linhas e colunas: {df_pib.shape}')
print(f'Exemplo de dados:')
print(df_pib.head())
print(f'Número de dados vazios: {np.count_nonzero(df_pib.isna().to_numpy())}')
print(f'Número de linhas com municípios vazios: {df_pib["local"].isna().sum()}')
print(f'Número de dados vazios após drop de linhas sem dados numéricos: {np.count_nonzero(df_pib.isna().to_numpy())}')
print(f'Lista de locais com PIB vazio:')
print(df_pib[df_pib['pib_corrente'].isna()]['local'].unique())
"""

# FILTRO DE ANOMALIAS: Substituir pib_corrente negativo por vazio (erro nos dados originais do IBGE)
mask_neg = df_pib['pib_corrente'] <= 0
pib_negativo = df_pib.loc[mask_neg]

if mask_neg.any():
    df_pib.loc[mask_neg, 'pib_corrente'] = np.nan
    print('pib_corrente negativo substituído por vazio.')
    print(f'Total de registros corrigidos: {mask_neg.sum()}')
    print('Lista de municípios/Ano com registros corrigidos:')
    print(pib_negativo[['local', 'ano', 'pib_corrente']])

# ! DataFrame não possui informações sobre os Estados associados, sendo aconselhado incluir de maneira objetiva
# Mapa oficial de codigos IBGE (UF) - https://www.ibge.gov.br/explica/codigos-dos-municipios.php
# Incluir nova coluna 'estado' utilizando a sigla do município presente na coluna 'local' (ex: "ARACAJU (SE)" -> "SE")
df_pib['estado'] = df_pib['local'].str.extract(r'\((\w{2})\)')[0]

# Drop linhas com Estado vazio, indicando que não há dados relevantes (somatórios ou totais)
df_pib.dropna(subset=['estado'], inplace=True)

#_ ## CONCLUSÃO SOBRE A QUALIDADE DOS DADOS DE PIB DO IBGE ###
#_ Após verificação, não há dados vazios inconsistentes no DataFrame de PIB do IBGE, o que é um bom sinal para a qualidade dos dados. 
#_ Havia um caso no município de GUAMARE (RN) em 2002 onde o valor de PIB corrente estava negativo, o que é um erro nos dados originais do IBGE. Este valor foi substituído por vazio (NaN) para evitar distorções nas análises futuras.
#_ As situações de PIB vazio estão associadas a períodos onde 10 municípios ainda não haviam sido criados, o que é compreensível e não indica um problema de qualidade dos dados.
#_ São eles:
#_ - AROEIRAS DO ITAIM: 2002, 2003, 2004 
#_ - BALNEARIO RINCAO: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 
#_ - FIGUEIRAO: 2002, 2003, 2004 
#_ - IPIRANGA DO NORTE: 2002, 2003, 2004 
#_ - ITANHANGA: 2002, 2003, 2004 
#_ - MOJUI DOS CAMPOS: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 
#_ - NAZARIA: 2002, 2003, 2004, 2005, 2006, 2007, 2008 
#_ - PARAISO DAS AGUAS: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 
#_ - PESCARIA BRAVA: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 
#_ - PINTO BANDEIRA: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012
#_ IMPORTANTE: Esta base é uma SÉRIE TEMPORAL (2002-2023), onde cada município aparece múltiplas vezes (uma linha por ano).
#_ As tipagens das colunas estão corretas, o que facilita a manipulação dos dados posteriormente. 
#_ O exemplo de dados mostra que as informações estão organizadas de forma semi-estruturada. Não existem códigos de correspondência para localidades no arquivo.
#_ Dessa forma, será necessário normalizar nomes e caracteres para junção das informações, além de incluir a informação de Estado
#_ As linhas vazias foram verificadas e não continham dados relevantes (somatórios ou totais), o que confirma que não há dados faltantes significativos no DataFrame.
#_ IMPORTANTE: Os valores de PIB encontram-se em estado CORRENTE (equivalente ao valor monetário de seu ano) e em MIL REAIS!
#_ Esta base do IBGE considera convenção antiga de Código de Município com 6 dígitos (sem o digito verificador).
#_##------------------------------------------------------###

# ! SCRIPT PARA UNIR BASES DE POPULAÇÃO E PIB - ANÁLISE DE CORRESPONDÊNCIA ENTRE MUNICÍPIOS
# Verificar correspondência entre os municípios das duas bases
pib_municipios = set(df_pib['local'])
hab_municipios = set(df_hab['municipio']) 

# TODO Veriricações opcionais
"""
print(f'\nCOMPARAÇÃO DE MUNICÍPIOS ENTRE AS BASES (ANTES DA NORMALIZAÇÃO):')
print(f'FONTE 1 (População) df_hab: {len(df_hab)} locais únicos')
print(f'FONTE 2 (PIB) df_pib: {len(pib_municipios)} locais únicos')
"""

# NORMALIZAR TEXTO PARA FACILITAR JUNÇÃO DOS DADOS
def remover_acentos(texto):
    if pd.isna(texto):
        return texto
    texto_nfd = unicodedata.normalize('NFD', texto)
    return ''.join(char for char in texto_nfd if unicodedata.category(char) != 'Mn')

def normalizar_municipio(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto).strip().replace('-', ' ')
    texto = ' '.join(texto.split())
    # Padronizar preposições
    for prep in [' DO ', ' DA ', ' DE ', ' DOS ', ' DAS ']:
        texto = texto.replace(prep, ' # ')
    return texto

# Aplicar as funções de normalização nas colunas de município
# Normalizar df_hab
df_hab['municipio_match'] = df_hab['municipio'].str.replace(r'\s*\(.*?\)\s*', ' ', regex=True)
df_hab['municipio_match'] = df_hab['municipio_match'].apply(remover_acentos).str.upper()
df_hab['municipio_match'] = df_hab['municipio_match'].apply(normalizar_municipio)

# Normalizar df_pib
df_pib['local_match'] = df_pib['local'].str.replace(r'\s*\(.*?\)\s*', ' ', regex=True)
df_pib['local_match'] = df_pib['local_match'].apply(remover_acentos).str.upper().str.strip()
df_pib['local_match'] = df_pib['local_match'].apply(normalizar_municipio)

# Análise final de correspondência entre os municípios das duas bases
# Considerar combinação única de Local_match + Estado para ambas as bases
pib_municipios = set(zip(df_pib['local_match'], df_pib['estado']))
hab_municipios = set(zip(df_hab['municipio_match'], df_hab['estado']))

# Identificar municípios que não têm correspondência
apenas_hab = hab_municipios - pib_municipios
apenas_pib = pib_municipios - hab_municipios
match = hab_municipios & pib_municipios

# TODO Veriricações opcionais
"""
print(f'\nCOMPARAÇÃO DE MUNICÍPIOS ENTRE AS BASES (APÓS NORMALIZAÇÃO):')
print(f'FONTE 1 (População) df_hab: {len(hab_municipios)} locais únicos')
print(f'FONTE 2 (PIB) df_pib: {len(pib_municipios)} locais únicos')
print(f'\nMatch entre as bases: {len(match)} locais únicos')
print(f'Municípios apenas em df_hab (não encontrados no PIB): {len(apenas_hab)}')
print(f'Municípios apenas em df_pib (não encontrados na População): {len(apenas_pib)}')
if apenas_hab:
    print(f"\nMunicípios em df_hab SEM correspondência no PIB:")
    for municipio, estado in sorted(apenas_hab):
        print(f"  {municipio} ({estado})")

if apenas_pib:
    print(f"\nMunicípios em df_pib SEM correspondência na População:")
    for municipio, estado in sorted(apenas_pib):
        print(f"  {municipio} ({estado})")
"""

# ! AJUSTE MANUAL DE CORRESPONDÊNCIAS POR ERRO DE GRAFIA
# Mapeamento manual - casos com diferença de grafia
mapeamento_hab = {
    'DONA EUSEBIA': 'DONA EUZEBIA',          # Diferença de grafia (Eusebia/Euzebia)
    'FLORINIA': 'FLORINEA',                  # Florinia/Florinea
    'GRACHO CARDOSO': 'GRACCHO CARDOSO',     # Gracho/Graccho
    'ITAPAGE': 'ITAPAJE',                    # Itapage/Itapaje
    'POXOREO': 'POXOREU',                    # Poxoreo/Poxoreu
    'SAO LUIS # PARAITINGA': 'SAO LUIZ # PARAITINGA',  # Luis vs Luiz
    'SAO THOME # LETRAS': 'SAO TOME # LETRAS',         # Thomé vs Tomé
    'AUGUSTO SEVERO': 'CAMPO GRANDE',                  # Mudança de nome histórica
    'FORTALEZA # TABOCAO': 'TABOCAO',                  # Cadastrado sem "Fortaleza do"
}

df_hab['municipio_match'] = df_hab['municipio_match'].replace(mapeamento_hab)

mapeamento_condicional = {
    ('SANTA TEREZINHA', 'PB'): 'SANTA TERESINHA',  # PB: convert Z to S
    ('SANTA TERESINHA', 'BA'): 'SANTA TEREZINHA',  # BA: convert S to Z
}

# Aplicar mapeamento condicional
for (municipio, estado), novo_nome in mapeamento_condicional.items():
    mask = (df_hab['municipio_match'] == municipio) & (df_hab['estado'] == estado)
    df_hab.loc[mask, 'municipio_match'] = novo_nome

# Verificação final
pib_municipios = set(zip(df_pib['local_match'], df_pib['estado']))
hab_municipios = set(zip(df_hab['municipio_match'], df_hab['estado']))

# Identificar municípios que não têm correspondência
apenas_hab = hab_municipios - pib_municipios
apenas_pib = pib_municipios - hab_municipios
match = hab_municipios & pib_municipios

# TODO Veriricações opcionais
"""
print(f'\nCOMPARAÇÃO DE MUNICÍPIOS ENTRE AS BASES (APÓS NORMALIZAÇÃO):')
print(f'FONTE 1 (População) df_hab: {len(hab_municipios)} locais únicos')
print(f'FONTE 2 (PIB) df_pib: {len(pib_municipios)} locais únicos')
print(f'\nMatch entre as bases: {len(match)} locais únicos')
print(f'Municípios apenas em df_hab (não encontrados em df_pib): {len(apenas_hab)}')
print(f'Municípios apenas em df_pib (não encontrados em df_hab): {len(apenas_pib)}')
if apenas_hab:
    print(f"\nMunicípios em df_hab SEM correspondência em df_pib:")
    for municipio, estado in sorted(apenas_hab):
        print(f"  {municipio} ({estado})")

if apenas_pib:
    print(f"\nMunicípios em df_pib SEM correspondência em df_hab:")
    for municipio, estado in sorted(apenas_pib):
        print(f"  {municipio} ({estado})")
"""

#_ ## CONCLUSÃO DOS AJUSTES MANUAIS E CORRESPONDÊNCIA ENTRE AS BASES ###
#_ Após ajustes manuais, o número de correspondências entre as bases é total
#_ Em ambas as bases, existem 5570 municípios únicos e 27 Estados com nomes únicos de correspondência.
#_ Esta base do IBGE considera convenção antiga de Código de Município com 6 dígitos (sem o digito verificador).
#_ ##----------------------------------------------------------------###

# ! CRIAR BASE FINAL UNIFICADA
# Transformar df_hab de formato WIDE (anos nas colunas) para formato LONG (anos nas linhas)
# Identificar colunas de anos (2002 a 2023)
anos_colunas = [str(ano) for ano in range(2002, 2024)]
colunas_id = ['codigo', 'municipio', 'estado', 'municipio_match']

# Verificar quais colunas de anos existem em df_hab
anos_existentes = [col for col in anos_colunas if col in df_hab.columns]

# Realizar o melt para transformar anos em linhas
df_hab_long = pd.melt(
    df_hab,
    id_vars=colunas_id,
    value_vars=anos_existentes,
    var_name='ano',
    value_name='populacao'
)

# Converter coluna Ano para string para compatibilidade com df_pib
df_hab_long['ano'] = pd.to_numeric(df_hab_long['ano'], errors='coerce')

# FILTRO DE ANOMALIAS: Substituir populacao negativa ou zero por NaN (erro nos dados originais)
populacao_invalida = df_hab_long[df_hab_long['populacao'] <= 0]
if len(populacao_invalida) > 0:
    df_hab_long.loc[df_hab_long['populacao'] <= 0, 'populacao'] = np.nan
    print(f'populacao negativa ou zero substituída por NaN.')
    print(f'Total de registros corrigidos: {len(populacao_invalida)}')
    print(f'Lista de municípios/Ano com registros corrigidos:')
    print(populacao_invalida[['municipio_match', 'ano', 'populacao']])

# Realizar junção entre os DataFrames utilizando municipio_match + estado + ano
df_final = pd.merge(
    df_hab_long,
    df_pib,
    left_on=['municipio_match', 'estado', 'ano'],
    right_on=['local_match', 'estado', 'ano'],
    how='inner'
)

# Ajustar formato final - Drop município, renomear municipio_match para municipio, drop local e local_match, converter municipio para string
df_final.drop(columns=['municipio', 'local', 'local_match'], inplace=True)
df_final.rename(columns={'municipio_match': 'municipio'}, inplace=True)
df_final['municipio'] = df_final['municipio'].astype('string')
df_final['estado'] = df_final['estado'].astype('string')

# Converter ano para numérico
df_final['ano'] = pd.to_numeric(df_final['ano'], errors='coerce')

# Reordenar colunas para facilitar visualização
colunas_principais = ['codigo', 'municipio', 'estado', 'ano', 'populacao', 'pib_corrente', 'va_industria_corrente', 'va_agropecuaria_corrente']
df_final = df_final[colunas_principais]

# Verificações finais
print(f'\nDataFrame final unificado de PIB e População:')
print(f'Número de municípios-Estado únicos: {df_final[["codigo", "estado"]].drop_duplicates().shape[0]}')
print(f'Anos disponíveis: {sorted(df_final["ano"].unique())}')

# VALIDAÇÃO: comparar totais de PIB para 2023
pib_original_2023 = df_pib[df_pib["ano"] == 2023]["pib_corrente"].sum()
pib_final_2023 = df_final[df_final["ano"] == 2023]["pib_corrente"].sum()

print(f'Total do PIB 2023 no df_pib original: {pib_original_2023:,.2f} (Mil Reais)')
print(f'Total do PIB 2023 no df_final (após merge): {pib_final_2023:,.2f} (Mil Reais)')
print(f'Diferença (original - final = ZERO): {pib_original_2023 - pib_final_2023:,.2f} (Mil Reais)')

# VALIDAÇÃO: comparar totais de População para 2023
pop_original_2023 = df_hab_long[df_hab_long["ano"] == 2023]["populacao"].sum()
pop_final_2023 = df_final[df_final["ano"] == 2023]["populacao"].sum()

print(f'Total da população 2023 no df_hab original: {pop_original_2023:,.0f} habitantes')
print(f'Total da população 2023 no df_final (após merge): {pop_final_2023:,.0f} habitantes')
print(f'Diferença (original - final = ZERO): {pop_original_2023 - pop_final_2023:,.0f} habitantes')

# APLICAR DEFLATORES EM PIB_corrente PARA AJUSTAR PIB PARA PIB REAL
# Carregar tabela de deflatores
tabela_deflatores = pq.read_table(Path(PROCESSED_DATA_PATH) / 'tabela_deflatores.parquet')
df_deflatores_temp = tabela_deflatores.to_pandas()

# Realizar merge com deflatores usando a coluna 'ano'
df_final = pd.merge(
    df_final,
    df_deflatores_temp,
    on='ano',
    how='left'
)

# Calcular PIB_real e va_industria_real usando os deflatores
df_final['pib_real'] = (df_final['pib_corrente'] * 100) / df_final['deflator_pib_2021']
df_final['va_industria_real'] = (df_final['va_industria_corrente'] * 100) / df_final['deflator_pib_industria_2021']
df_final['va_agropecuaria_real'] = (df_final['va_agropecuaria_corrente'] * 100) / df_final['deflator_pib_agropecuaria_2021']
df_final = df_final.drop(columns=['deflator_pib_2021', 'deflator_pib_industria_2021', 'deflator_pib_agropecuaria_2021'])

# FILTRO DE ANOMALIAS: Substituir pib_real negativo ou zero por NaN (inconsistência nos dados)
#! Atenção: Atividade industrial e agropecuária pode ter valor adicionado negativo, o que é permitido estruturalmente.
pib_real_invalido = df_final[df_final['pib_real'] <= 0]
if len(pib_real_invalido) > 0:
    df_final.loc[df_final['pib_real'] <= 0, 'pib_real'] = np.nan
    print(f'pib_real negativo ou zero substituído por NaN.')
    print(f'Total de registros corrigidos: {len(pib_real_invalido)}')
    print(f'Lista de municípios/Ano com registros corrigidos:')
    print(pib_real_invalido[['local', 'ano', 'pib_real']])

# Verificação dos novos dados
print(f'\nDados após aplicação dos deflatores:')
print(f'Número de linhas e colunas: {df_final.shape}')
print(f'Tipagem das colunas:\n{df_final.dtypes}')

# Gerar arquivo Parquet da base final unificada com pyarrow
tabela_final = pa.Table.from_pandas(df_final)
pq.write_table(tabela_final, Path(PROCESSED_DATA_PATH) / 'base_pib_hab.parquet', compression='snappy')

# Liberação de memória
del df_pib, mask_neg, pib_negativo, pib_municipios, hab_municipios, df_hab, apenas_hab, match, mapeamento_hab, mapeamento_condicional, anos_colunas, colunas_id, anos_existentes, df_hab_long, populacao_invalida, df_final, colunas_principais, pib_original_2023, pib_final_2023, pop_original_2023, pop_final_2023, tabela_deflatores, df_deflatores_temp, pib_real_invalido, tabela_final
gc.collect()

#_ ## CONCLUSÃO DA BASE_PIB_HAB ###
#_ Após ajustes, estão presentes valores de PIB a preços correntes e constantes e valor adicionado para indústria a preços correntes e constantes.
#_ VALORES EM MIL REAIS E VALORES CONSTANTES (REAIS) NA BASE DE EQUIVALÊNCIA EM 2023.
#_ Valores per capita calculados para PIB e valor adicionado da indústria, ambos a preços constantes (reais) na base 2023.
#_ Esta base do IBGE considera convenção antiga de Código de Município com 6 dígitos (sem o digito verificador).
#_ ##---------------------------###
# %% DADOS DE DESEMBOLSOS DO BNDES PARA OS MUNICÍPIOS BRASILEIROS
# INGESTÃO DE DADOS 4 DE 4 - DADOS DE DESEMBOLSOS DO BNDES
# Fonte 5: Dados de Desembolso do BNDES - Série histórica de desembolsos mensais do BNDES por município, setor e modalidade de crédito
# https://dadosabertos.bndes.gov.br/dataset/desembolsos-mensais/resource/179950b8-b504-4cc7-b0db-9c9eed99e9ba

# importar arquivo de desembolsos mensais do BNDES
df_bndes = pd.read_csv(Path(RAW_DATA_PATH) / 'desembolsos_mensais.csv', encoding='utf-8', sep=',')

# Converter todas as colunas para string[python], exceto desembolsos_reais (float64)
for col in df_bndes.columns:
    if col != 'desembolsos_reais':
        df_bndes[col] = df_bndes[col].astype('string')

# Garantir desembolsos_reais como float64 e em mil reais (dividir por 1000)
if 'desembolsos_reais' in df_bndes.columns:
    df_bndes['desembolsos_reais'] = df_bndes['desembolsos_reais'].astype('float64')
    df_bndes['desembolsos_reais'] = df_bndes['desembolsos_reais'] / 1000
    df_bndes = df_bndes.rename(columns={'desembolsos_reais': 'desembolsos_corrente'})

# Ajuste de classificação de setor_cnae para reclassificar subsetores de indústria corretamente
def reclassificar_setor_cnae_1(row):
    if row['setor_cnae'] == 'COMÉRCIO E SERVIÇOS' and row['subsetor_cnae_agrupado'] in ['ELETRICIDADE E GÁS', 'ÁGUA, ESGOTO E LIXO']:
        return 'INDÚSTRIA DE UTILIDADES PÚBLICAS'
    return row['setor_cnae']

# Ajuste de classificação de setor_cnae para reclassificar subsetores de indústria corretamente
def reclassificar_setor_cnae_2(row):
    if row['setor_cnae'] == 'COMÉRCIO E SERVIÇOS' and row['subsetor_cnae_agrupado'] in ['CONSTRUÇÃO']:
        return 'INDÚSTRIA DE CONSTRUÇÃO'
    return row['setor_cnae']

# Aplicar a função de reclassificação
df_bndes['setor_cnae'] = df_bndes.apply(reclassificar_setor_cnae_1, axis=1)
df_bndes['setor_cnae'] = df_bndes.apply(reclassificar_setor_cnae_2, axis=1)

# Print total de linhas reclassificadas
linhas_reclassificadas = df_bndes[(df_bndes['setor_cnae'] == 'INDÚSTRIA DE UTILIDADES PÚBLICAS') | (df_bndes['setor_cnae'] == 'INDÚSTRIA DE CONSTRUÇÃO')].shape[0]
print(f'Percentual de linhas reclassificadas por erro de setor_cnae: {linhas_reclassificadas} linhas reclassificadas, representando {linhas_reclassificadas / df_bndes.shape[0] * 100:.2f}% do total de linhas.')

# Drop de colunas não relevantes para análise ('_id', 'instrumento_financeiro', 'inovacao', 'regiao', 'subsetor_cnae_agrupado', 'setor_bndes', 'subsetor_bndes')
colunas_para_drop = ['_id', 'instrumento_financeiro', 'inovacao', 'regiao', 'subsetor_cnae_agrupado', 'setor_bndes', 'subsetor_bndes']
colunas_existentes_para_drop = [col for col in colunas_para_drop if col in df_bndes.columns]
df_bndes.drop(columns=colunas_existentes_para_drop, inplace=True)

# Drop de informações fora do período de interesse (2002-2023)
df_bndes = df_bndes[df_bndes['ano'].isin([str(ano) for ano in range(2002, 2024)])]

# Groupby de desembolsos_corrente para transformar informações mensais em anuais, somando os desembolsos por ano, município, uf
df_bndes_total = df_bndes.groupby(['ano', 'municipio_codigo', 'municipio', 'uf'], as_index=False)['desembolsos_corrente'].sum()

# TODO Verificações opcionais
"""
print(f'\nDataFrame de desembolsos do BNDES:')
print(f'Número de linhas e colunas: {df_bndes.shape}')
print(f'Tipagens das colunas:\n{df_bndes.dtypes}')
print(f'Exemplo de dados:')
print(df_bndes.head())
"""

# APLICAR DEFLATORES EM DESEMBOLSOS PARA AJUSTAR DESEMBOLSO CORRENTE PARA VALOR REAL
# Carregar tabela de deflatores
tabela_deflatores = pq.read_table(Path(PROCESSED_DATA_PATH) / 'tabela_deflatores.parquet')
df_deflatores_temp = tabela_deflatores.to_pandas()

# Converter ano para numérico para compatibilidade com merge
df_bndes_total['ano'] = pd.to_numeric(df_bndes_total['ano'], errors='coerce')

# Realizar merge com deflatores usando left_on='ano' e right_on='ano'
df_bndes_total_deflacionado = pd.merge(
    df_bndes_total,
    df_deflatores_temp,
    left_on='ano',
    right_on='ano',
    how='left'
)

# Calcular desembolsos reais usando o deflator do PIB geral
df_bndes_total_deflacionado['desembolsos_real'] = (df_bndes_total_deflacionado['desembolsos_corrente'] * 100) / df_bndes_total_deflacionado['deflator_pib_2021']
df_bndes_total_deflacionado = df_bndes_total_deflacionado.drop(columns=['deflator_pib_2021', 'deflator_pib_industria_2021', 'deflator_pib_agropecuaria_2021'])

# Verificação dos dados deflacionados para todos os setores
print(f'\nDataFrame de desembolsos do BNDES após consolidação e deflação:')
print(f'Número de linhas e colunas: {df_bndes_total_deflacionado.shape}')
print(f'Tipagens das colunas:\n{df_bndes_total_deflacionado.dtypes}')

# Gerar arquivo Parquet da base de desembolsos do BNDES com pyarrow - todos os setores
tabela_bndes_total = pa.Table.from_pandas(df_bndes_total_deflacionado)
pq.write_table(tabela_bndes_total, Path(PROCESSED_DATA_PATH) / 'base_bndes_total.parquet', compression='snappy')

# Groupby de desembolsos_corrente para transformar informações mensais em anuais, somando os desembolsos por ano, município, uf e mantendo abertura apenas por setor_cnae associado à indústria (INDÚSTRIA DE TRANSFORMAÇÃO e INDÚSTRIA EXTRATIVA)
df_bndes_industria = df_bndes[df_bndes['setor_cnae'].isin(['INDÚSTRIA DE TRANSFORMAÇÃO', 'INDÚSTRIA EXTRATIVA', 'INDÚSTRIA DE UTILIDADES PÚBLICAS', 'INDÚSTRIA DE CONSTRUÇÃO'])].groupby(['ano', 'municipio_codigo', 'municipio', 'uf', 'setor_cnae'], as_index=False)['desembolsos_corrente'].sum()

# Converter ano para numérico para compatibilidade com merge
df_bndes_industria['ano'] = pd.to_numeric(df_bndes_industria['ano'], errors='coerce')

# Realizar merge com deflatores usando left_on='ano' e right_on='ano'
df_bndes_industria_deflacionado = pd.merge(
    df_bndes_industria,
    df_deflatores_temp,
    left_on='ano',
    right_on='ano',
    how='left'
)

# Calcular desembolsos reais usando o deflator do PIB da indústria
df_bndes_industria_deflacionado['desembolsos_industria_real'] = (df_bndes_industria_deflacionado['desembolsos_corrente'] * 100) / df_bndes_industria_deflacionado['deflator_pib_industria_2021']
df_bndes_industria_deflacionado = df_bndes_industria_deflacionado.drop(columns=['deflator_pib_2021', 'deflator_pib_industria_2021', 'deflator_pib_agropecuaria_2021', 'setor_cnae'])
df_bndes_industria_deflacionado = df_bndes_industria_deflacionado.rename(columns={'desembolsos_corrente': 'desembolsos_industria_corrente'})
# Verificação dos dados deflacionados para todos os setores
print(f'\nDataFrame de desembolsos do BNDES para Indústria após consolidação e deflação:')
print(f'Número de linhas e colunas: {df_bndes_industria_deflacionado.shape}')
print(f'Tipagens das colunas:\n{df_bndes_industria_deflacionado.dtypes}')

# Gerar arquivo Parquet da base de desembolsos do BNDES com pyarrow - apenas indústria
tabela_bndes_industria = pa.Table.from_pandas(df_bndes_industria_deflacionado)
pq.write_table(tabela_bndes_industria, Path(PROCESSED_DATA_PATH) / 'base_bndes_industria.parquet', compression='snappy')

# Groupby de desembolsos_corrente para transformar informações mensais em anuais, somando os desembolsos por ano, município, uf e mantendo abertura apenas por setor_cnae associado à AGROPECUÁRIA (AGROPECUÁRIA)
df_bndes_agropecuaria = df_bndes[df_bndes['setor_cnae'].isin(['AGROPECUÁRIA'])].groupby(['ano', 'municipio_codigo', 'municipio', 'uf', 'setor_cnae'], as_index=False)['desembolsos_corrente'].sum()

# Converter ano para numérico para compatibilidade com merge
df_bndes_agropecuaria['ano'] = pd.to_numeric(df_bndes_agropecuaria['ano'], errors='coerce')

# Realizar merge com deflatores usando left_on='ano' e right_on='ano'
df_bndes_agropecuaria_deflacionado = pd.merge(
    df_bndes_agropecuaria,
    df_deflatores_temp,
    left_on='ano',
    right_on='ano',
    how='left'
)

# Calcular desembolsos reais usando o deflator do PIB da agropecuária
df_bndes_agropecuaria_deflacionado['desembolsos_agropecuaria_real'] = (df_bndes_agropecuaria_deflacionado['desembolsos_corrente'] * 100) / df_bndes_agropecuaria_deflacionado['deflator_pib_agropecuaria_2021']
df_bndes_agropecuaria_deflacionado = df_bndes_agropecuaria_deflacionado.drop(columns=['deflator_pib_2021', 'deflator_pib_industria_2021', 'deflator_pib_agropecuaria_2021', 'setor_cnae'])
df_bndes_agropecuaria_deflacionado = df_bndes_agropecuaria_deflacionado.rename(columns={'desembolsos_corrente': 'desembolsos_agropecuaria_corrente'})

# Verificação dos dados deflacionados para todos os setores
print(f'\nDataFrame de desembolsos do BNDES para Agropecuária após consolidação e deflação:')
print(f'Número de linhas e colunas: {df_bndes_agropecuaria_deflacionado.shape}')
print(f'Tipagens das colunas:\n{df_bndes_agropecuaria_deflacionado.dtypes}')

# Gerar arquivo Parquet da base de desembolsos do BNDES com pyarrow - apenas agropecuária
tabela_bndes_agropecuaria = pa.Table.from_pandas(df_bndes_agropecuaria_deflacionado)
pq.write_table(tabela_bndes_agropecuaria, Path(PROCESSED_DATA_PATH) / 'base_bndes_agropecuaria.parquet', compression='snappy')

# Liberação de memória
del df_bndes, linhas_reclassificadas, colunas_para_drop, colunas_existentes_para_drop, df_bndes_total, tabela_deflatores, df_bndes_total_deflacionado, tabela_bndes_total, df_bndes_agropecuaria, df_bndes_agropecuaria_deflacionado, tabela_bndes_agropecuaria
gc.collect()

#_ ## CONCLUSÃO SOBRE A QUALIDADE DOS DADOS DE DESEMBOLSO DO BNDES ###
#_ Após verificação, concluiu-se que em determinadas situações é utilizado o código de município '999999', que representa desembolsos não-localizáveis, esse evento não é ideal, mas pode ser aceito com as devidas precauções (envolve aprox. 743 bilhões em mil reais - valores em 2023). 
#_ As tipagens das colunas estão corretas, o que facilita a manipulação dos dados posteriormente. 
#_ O exemplo de dados mostra que as informações estão organizadas de forma estruturada. Existem códigos de correspondência para localidades no arquivo.
#_ Foi realizado um SELECT para o período de interesse (2002-2023) e um GROUPBY para transformar as informações mensais em anuais, somando os desembolsos por ano, município e uf.
#_ Foram gerados dois arquivos Parquet: um com os desembolsos totais por município e ano para todos os setores e outro apenas para o setor de indústria.
#_ IMPORTANTE: Os valores de desembolsos encontram-se em estado CORRENTE (equivalente ao valor monetário de seu ano), CONSTANTES/REAIS e em MIL REAIS!
#_ No arquivo base_bndes_total.parquet, os desembolsos foram ajustados utilizando o deflator do PIB geral, enquanto no arquivo base_bndes_industria.parquet, os desembolsos foram ajustados utilizando o deflator específico do PIB industrial.
#_ O código de município usado pelo BNDES é o formato atual oficial de 7 dígitos (com o dígito verificador), enquanto o código de município presente na base do IBGE é o formato antigo de 6 dígitos (sem o dígito verificador).
#_##--------------------------------------------------------------###
# %% VERIFICAR SE TODOS OS PARQUETS POSSUEM DADOS SUFICIENTES EM SEUS ANOS
# TODO Verificações opcionais para garantir que os arquivos Parquet gerados possuem dados suficientes em seus anos
"""
# Verificar anos disponíveis em base_pib_hab.parquet
tabela_pib_hab = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_pib_hab.parquet')
df_pib_hab = tabela_pib_hab.to_pandas()
print(f'\nAnos disponíveis em base_pib_hab.parquet: {sorted(df_pib_hab["ano"].unique())}')
print(f'Total do PIB por ano em base_pib_hab.parquet:')
print(df_pib_hab.groupby('ano')['pib_corrente'].sum())
print(f'Total da população por ano em base_pib_hab.parquet:')
print(df_pib_hab.groupby('ano')['populacao'].sum())
print(f'------------------------------------------------')

# Verificar anos disponíveis em base_bndes_total.parquet
tabela_bndes_total = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_total.parquet')
df_bndes_total = tabela_bndes_total.to_pandas()
print(f'\nAnos disponíveis em base_bndes_total.parquet: {sorted(df_bndes_total["ano"].unique())}')
print(f'Total de desembolsos por ano em base_bndes_total.parquet:')
print(df_bndes_total.groupby('ano')['desembolsos_corrente'].sum())
print(f'------------------------------------------------')

# Verificar anos disponíveis em base_bndes_industria.parquet
tabela_bndes_industria = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_industria.parquet')
df_bndes_industria = tabela_bndes_industria.to_pandas()
print(f'\nAnos disponíveis em base_bndes_industria.parquet: {sorted(df_bndes_industria["ano"].unique())}')
print(f'Total de desembolsos por ano em base_bndes_industria.parquet:')
print(df_bndes_industria.groupby('ano')['desembolsos_corrente'].sum())
print(f'------------------------------------------------')

# Verificar anos disponíveis em base_bndes_agropecuaria.parquet
tabela_bndes_agropecuaria = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_agropecuaria.parquet')
df_bndes_agropecuaria = tabela_bndes_agropecuaria.to_pandas()
print(f'\nAnos disponíveis em base_bndes_agropecuaria.parquet: {sorted(df_bndes_agropecuaria["ano"].unique())}')
print(f'Total de desembolsos por ano em base_bndes_agropecuaria.parquet:')
print(df_bndes_agropecuaria.groupby('ano')['desembolsos_corrente'].sum())
print(f'------------------------------------------------')

# Liberação de memória
del tabela_pib_hab, df_pib_hab, tabela_bndes_total, df_bndes_total, tabela_bndes_industria, df_bndes_industria, tabela_bndes_agropecuaria, df_bndes_agropecuaria
gc.collect()
"""
# %% PAINEL DE DADOS 1 - MODELO 1 - PIB real e desembolsos do BNDES (nível município-ano)
# ANÁLISE 1: MODELO PRINCIPAL - Evolução do PIB real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)

# Carregar arquivos parquet para análise
tabela_pib_hab = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_pib_hab.parquet')
df_pib_hab = tabela_pib_hab.to_pandas()

tabela_bndes_total = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_total.parquet')
df_bndes_total = tabela_bndes_total.to_pandas()

# Preparar df_pib_hab: selecionar colunas relevantes para merge
df_pib_merge = df_pib_hab[['codigo', 'municipio', 'estado', 'ano', 'populacao', 'pib_corrente', 'pib_real', 'va_industria_corrente', 'va_industria_real', 'va_agropecuaria_corrente', 'va_agropecuaria_real']].copy()

# Preparar df_bndes_total: renomear 'municipio_codigo' e 'ano' para compatibilidade
df_bndes_merge = df_bndes_total[['municipio_codigo', 'municipio', 'uf', 'ano', 'desembolsos_corrente', 'desembolsos_real']].copy()
df_bndes_merge.rename(columns={'municipio_codigo': 'codigo'}, inplace=True)

# Converter para string para garantir compatibilidade no merge
df_pib_merge['codigo'] = df_pib_merge['codigo'].astype('string')
df_pib_merge['ano'] = df_pib_merge['ano'].astype('string')
df_bndes_merge['codigo'] = df_bndes_merge['codigo'].astype('string')
df_bndes_merge['ano'] = df_bndes_merge['ano'].astype('string')

# Mapa de conversão: nome completo do estado -> sigla (para compatibilidade com PIB)
estado_map = {
    'RONDONIA': 'RO', 'ACRE': 'AC', 'AMAZONAS': 'AM', 'RORAIMA': 'RR', 'PARA': 'PA', 
    'AMAPA': 'AP', 'TOCANTINS': 'TO', 'MARANHAO': 'MA', 'PIAUI': 'PI', 'CEARA': 'CE', 
    'RIO GRANDE DO NORTE': 'RN', 'PARAIBA': 'PB', 'PERNAMBUCO': 'PE', 'ALAGOAS': 'AL', 
    'SERGIPE': 'SE', 'BAHIA': 'BA', 'MINAS GERAIS': 'MG', 'ESPIRITO SANTO': 'ES', 
    'RIO DE JANEIRO': 'RJ', 'SAO PAULO': 'SP', 'PARANA': 'PR', 'SANTA CATARINA': 'SC', 
    'RIO GRANDE DO SUL': 'RS', 'MATO GROSSO DO SUL': 'MS', 'MATO GROSSO': 'MT', 
    'GOIAS': 'GO', 'DISTRITO FEDERAL': 'DF'
}

# Converter UF do BNDES para sigla
df_bndes_merge['uf'] = df_bndes_merge['uf'].map(estado_map).astype('string')

# Extrair primeiros 6 dígitos do código BNDES (drop do dígito verificador) para compatibilidade com código do IBGE
df_bndes_merge['codigo'] = df_bndes_merge['codigo'].str[:6]

# Agregar desembolsos por Código + uf + Ano antes do merge
df_bndes_merge = df_bndes_merge.groupby(['codigo', 'uf', 'ano'], as_index=False)[
    ['municipio', 'desembolsos_corrente', 'desembolsos_real']
].sum()

# LEFT MERGE: manter todos os registros de df_pib_hab e unir com df_bndes_total quando houver correspondência
# Usar Código + Estado/uf + Ano como chaves de junção
df_painel1 = pd.merge(
    df_pib_merge,
    df_bndes_merge,
    left_on=['codigo', 'estado', 'ano'],
    right_on=['codigo', 'uf', 'ano'],
    how='left',
    suffixes=('_pib', '_bndes')
)

# Preencher valores de desembolso com zero onde não houver correspondência
df_painel1['desembolsos_real'] = df_painel1['desembolsos_real'].fillna(0)
df_painel1['desembolsos_corrente'] = df_painel1['desembolsos_corrente'].fillna(0)

# Consolidar coluna de município: usar municipio_pib quando disponível, caso contrário usar municipio_bndes
df_painel1['municipio'] = df_painel1['municipio_pib'].fillna(df_painel1['municipio_bndes'])

# Remover colunas redundantes
df_painel1.drop(columns=['uf', 'municipio_pib', 'municipio_bndes'], inplace=True)

# verificar se o total de desembolsos ajustados no DataFrame de análise é igual ao total de desembolsos ajustados na base do BNDES
total_desembolsos_ajustados_analise = df_painel1['desembolsos_real'].sum()
total_desembolsos_ajustados_bndes = df_bndes_merge['desembolsos_real'].sum()
total_desembolsos_ajustados_bndes_999999 = df_bndes_merge[df_bndes_merge["codigo"] == "999999"]["desembolsos_real"].sum()

print(f'\nDataFrame de análise (município-ano):')
print(f'\nTotal de desembolsos ajustados no DataFrame de análise: {total_desembolsos_ajustados_analise:,.2f} (Mil Reais)')
print(f'Total de desembolsos ajustados na base do BNDES: {total_desembolsos_ajustados_bndes:,.2f} (Mil Reais)')
print(f'Total de desembolsos com código de município 999999 (não localizáveis) na base do BNDES: {total_desembolsos_ajustados_bndes_999999:,.2f} (Mil Reais)')
print(f'Diferença (análise - BNDES): {total_desembolsos_ajustados_analise - total_desembolsos_ajustados_bndes + total_desembolsos_ajustados_bndes_999999:,.2f} (Mil Reais)')

# verificar se o total de PIB real no DataFrame de análise é igual ao total de PIB real na base do IBGE
total_pib_real_analise = df_painel1['pib_real'].sum()
total_pib_real_ibge = df_pib_merge['pib_real'].sum()
print(f'\nTotal de PIB real no DataFrame de análise: {total_pib_real_analise:,.2f} (Mil Reais)')
print(f'Total de PIB real na base do IBGE: {total_pib_real_ibge:,.2f} (Mil Reais)')
print(f'Diferença (análise - IBGE): {total_pib_real_analise - total_pib_real_ibge:,.2f} (Mil Reais)')

# verificar o número de municípios-estados únicos em 2023
municipios_estados_2023 = df_painel1[df_painel1['ano'] == '2023'][['codigo', 'estado']].drop_duplicates()
print(f'\nNúmero de municípios-estados únicos em 2023: {municipios_estados_2023.shape[0]}')
print(f'Número de municípios-estados únicos em 2023 com desembolsos > 0: {df_painel1[(df_painel1["ano"] == "2023") & (df_painel1["desembolsos_real"] > 0)].shape[0]}')

# verificar se o total de população no DataFrame de análise é igual ao total de população na base do IBGE
total_populacao_analise = df_painel1['populacao'].sum()
total_populacao_ibge = df_pib_merge['populacao'].sum()
print(f'\nTotal de população no DataFrame de análise: {total_populacao_analise:,.0f} habitantes')
print(f'Total de população na base do IBGE: {total_populacao_ibge:,.0f} habitantes')
print(f'Diferença (análise - IBGE): {total_populacao_analise - total_populacao_ibge:,.0f} habitantes')

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel1['ano'] = pd.to_numeric(df_painel1['ano'], errors='coerce')
df_painel1 = df_painel1.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável log_pib_real e variável dependente Y = delta_log_pib_real em estrutura LONG
df_painel1['log_pib_real'] = np.log(df_painel1['pib_real'])
df_painel1['delta_log_pib_real'] = df_painel1.groupby(['codigo', 'estado'])['log_pib_real'].diff(1)

# Calcular variável independente X = share_desembolso_real_pib_real_ano_anterior
# ! variável sem log, shift apenas no denominador (t-1, com fallback para t-2)
pib_lag1 = df_painel1.groupby(['codigo','estado'])['pib_real'].shift(1)
pib_lag2 = df_painel1.groupby(['codigo','estado'])['pib_real'].shift(2)

# ! Usar t-1 como padrão, mas quando t-1 for NaN ou zero, usar t-2
# Ocorre apenas em 1 caso, GUAMARE (RN) com PIB NEGATIVO em 2012
pib_lag = pib_lag1.copy()
mask_usar_lag2 = (pib_lag1.isna()) | (pib_lag1 == 0)
pib_lag[mask_usar_lag2] = pib_lag2[mask_usar_lag2]

df_painel1['share_desembolso_real_pib_real_ano_anterior'] = (df_painel1['desembolsos_real'] / pib_lag)

# Calcular variáveis lag 1--3 da variável independente X
for lag in range(1, 4):
    df_painel1[f'share_desembolso_real_pib_real_ano_anterior_lag{lag}'] = df_painel1.groupby(['codigo','estado'])['share_desembolso_real_pib_real_ano_anterior'].shift(lag)

# Calcular variáveis de controle: log_populacao_lag1, log_pibpc_real_lag1 e share_industria_lag1 (ou seja, em t-1)
df_painel1['log_populacao'] = np.log(df_painel1['populacao'])
df_painel1['log_populacao_lag1'] = df_painel1.groupby(['codigo','estado'])['log_populacao'].shift(1)
df_painel1['pibpc_real'] = df_painel1['pib_real'] / df_painel1['populacao']
df_painel1['log_pibpc_real'] = np.log(df_painel1['pibpc_real'])
df_painel1['log_pibpc_real_lag1'] = df_painel1.groupby(['codigo','estado'])['log_pibpc_real'].shift(1)
df_painel1['share_industria'] = df_painel1['va_industria_real'] / df_painel1['pib_real']
df_painel1['share_industria_lag1'] = df_painel1.groupby(['codigo','estado'])['share_industria'].shift(1)
df_painel1['share_agropecuaria'] = df_painel1['va_agropecuaria_real'] / df_painel1['pib_real']
df_painel1['share_agropecuaria_lag1'] = df_painel1.groupby(['codigo','estado'])['share_agropecuaria'].shift(1)

# TODO Verificações opcionais
# Verificar se todos os 5570 municipios possuem informações de PIB real e desembolsos do BNDES para todos os anos entre 2002 e 2023
municipios_anos = df_painel1.groupby(['codigo', 'estado'])['ano'].nunique().reset_index()
municipios_anos_completo = municipios_anos[municipios_anos['ano'] == 22]  # 22 anos entre 2002 e 2023
print(f'\nNúmero de municípios-estados com informações completas para todos os anos (2002-2023): {municipios_anos_completo.shape[0]}')
print(f'Número de municípios-estados com informações incompletas para o período (2002-2023): {municipios_anos[municipios_anos["ano"] < 22].shape[0]}')

# Listar os municípios-estados com informações incompletas para o período (2002-2023)
municipios_incompletos = municipios_anos[municipios_anos['ano'] < 22]
# Merge com df_painel1 para trazer o nome do município
municipios_incompletos = municipios_incompletos.merge(
    df_painel1[['codigo', 'estado', 'municipio']].drop_duplicates(),
    on=['codigo', 'estado'],
    how='left'
)
print(f'\nMunicípios-estados com informações incompletas para o período (2002-2023):')
for index, row in municipios_incompletos.iterrows():
    print(f'Código: {row["codigo"]}, Município: {row["municipio"]}, Estado: {row["estado"]}, Anos disponíveis: {row["ano"]}')
# NOTA: Municípios com menos de 22 anos de dados correspondem a municípios criados ao longo da série histórica

# Reordenar colunas para facilitar conferência e análise
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'log_pib_real', 'delta_log_pib_real', 'desembolsos_corrente', 'desembolsos_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'log_populacao', 'log_populacao_lag1', 'pibpc_real', 'log_pibpc_real', 'log_pibpc_real_lag1', 'va_industria_corrente', 'va_industria_real', 'share_industria', 'share_industria_lag1', 'va_agropecuaria_corrente', 'va_agropecuaria_real', 'share_agropecuaria', 'share_agropecuaria_lag1']
colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel1.columns]
df_painel1 = df_painel1[colunas_reordenadas_existentes]

# MODELO COMPLEMENTAR - Robustez - Evolução do PIB real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)
# Inseridas leads da variável independente para análise de efeitos antecipados

# Carregar arquivos parquet para análise
df_painel1c = df_painel1.copy()  # Usar o painel1 já preparado como base para painel1c, que é o modelo complementar com leads

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel1c['ano'] = pd.to_numeric(df_painel1c['ano'], errors='coerce')
df_painel1c = df_painel1c.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável independente lead (xt+1) e (xt+2)
# ! variável sem log, shift no numerador para ano futuro
df_painel1c.loc[df_painel1c['pib_real'] <= 0, 'pib_real'] = np.nan  # Substituir valores de PIB real menores ou iguais a zero por NaN para evitar problemas de divisão e log
pib_t = df_painel1c['pib_real']  # PIB_t
pib_tp1 = df_painel1c.groupby(['codigo','estado'])['pib_real'].shift(-1) # PIB_{t+1}
desemb_tp1 = df_painel1c.groupby(['codigo','estado'])['desembolsos_real'].shift(-1) # Desemb_{t+1}
desemb_tp2 = df_painel1c.groupby(['codigo','estado'])['desembolsos_real'].shift(-2) # Desemb_{t+2}

df_painel1c['share_desembolso_real_pib_real_ano_anterior_lead1'] = desemb_tp1 / pib_t
df_painel1c['share_desembolso_real_pib_real_ano_anterior_lead2'] = desemb_tp2 / pib_tp1
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'log_pib_real', 'delta_log_pib_real', 'desembolsos_corrente', 'desembolsos_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'share_desembolso_real_pib_real_ano_anterior_lead1', 'share_desembolso_real_pib_real_ano_anterior_lead2', 'log_populacao', 'log_populacao_lag1', 'pibpc_real', 'log_pibpc_real', 'log_pibpc_real_lag1', 'va_industria_corrente', 'va_industria_real', 'share_industria', 'share_industria_lag1', 'va_agropecuaria_corrente', 'va_agropecuaria_real', 'share_agropecuaria', 'share_agropecuaria_lag1']

colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel1c.columns]
df_painel1c_final = df_painel1c[colunas_reordenadas_existentes]

# Verificação final do DataFrame de análise com tipos de dados
print(f'\nDataFrame Painel 1 (antes do drop de NA):')
print(f'Número de linhas e colunas: {df_painel1.shape}')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel1["ano"].min()} - {df_painel1["ano"].max()}')

# Realizar o drop de registros NA (devido a lags) e comparar quantidade de informações perdidas nas pontas
df_painel1_final = df_painel1.dropna()

print(f'\nDataFrame Painel 1 após drop de NA (devido a lags):')
print(f'Número de linhas eliminadas: {df_painel1.shape[0] - df_painel1_final.shape[0]}, equivalente a {((df_painel1.shape[0] - df_painel1_final.shape[0]) / df_painel1.shape[0]) * 100:.2f}% do total de linhas')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel1_final["ano"].min()} - {df_painel1_final["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel1_final)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel1.parquet', compression='snappy')

# Realizar o drop de registros NA (devido a lags) e comparar quantidade de informações perdidas nas pontas
df_painel1c_final = df_painel1c_final.dropna()

print(f'\nDataFrame Painel 1C após drop de NA (devido a lags e leads):')
print(f'Número de linhas eliminadas: {df_painel1c_final.shape[0] - df_painel1.shape[0]}, equivalente a {((df_painel1c_final.shape[0] - df_painel1.shape[0]) / df_painel1.shape[0]) * 100:.2f}% do total de linhas do painel 1 original.')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel1c_final["ano"].min()} - {df_painel1c_final["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel1c_final)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel1c.parquet', compression='snappy')

print(f'Tipagens das colunas (1C):\n{df_painel1c_final.dtypes}')

# TODO Verificações opcionais para garantir que o painel de análise 1 possui dados suficientes para o período de 2006-2021
"""
df_painel1_final['ano'] = df_painel1_final['ano'].astype('string')
anos_disponiveis = sorted(df_painel1_final['ano'].dropna().unique())
print(f'\nAnos disponíveis no painel de análise 1: {anos_disponiveis}')
print('Total de linhas por ano no painel de análise 1:')
print(df_painel1_final['ano'].value_counts().sort_index())

# Tabela de faltantes: municipios-estado nas linhas e anos nas colunas
anos_periodo = [str(ano) for ano in range(2006, 2022)]
municipios_estados = df_painel1_final[['codigo', 'municipio', 'estado']].drop_duplicates()
base_anos = pd.DataFrame({'ano': anos_periodo})
base_anos['chave'] = 1
municipios_estados['chave'] = 1

grid = municipios_estados.merge(base_anos, on='chave', how='outer').drop(columns=['chave'])
presenca = df_painel1_final[['codigo', 'estado', 'ano']].drop_duplicates()
presenca['tem_dado'] = 'X'

tabela_faltantes = grid.merge(presenca, on=['codigo', 'estado', 'ano'], how='left')
tabela_faltantes['tem_dado'] = tabela_faltantes['tem_dado'].fillna('')

tabela_pivot = tabela_faltantes.pivot_table(
    index=['codigo', 'municipio', 'estado'],
    columns='ano',
    values='tem_dado',
    aggfunc='first',
    fill_value=''
).reset_index()

tabela_pivot = tabela_pivot[tabela_pivot[anos_periodo].eq('').any(axis=1)]

print('\nTabela de faltantes (apenas municipios-estado com pelo menos 1 ano faltante):')
if tabela_pivot.empty:
    print('Nenhum municipio-estado com anos faltantes no periodo 2006-2021.')
else:
    max_rows = 50
    if len(tabela_pivot) > max_rows:
        print(tabela_pivot.head(max_rows).to_string(index=False))
        print(f'\nExibindo {max_rows} de {len(tabela_pivot)} linhas.')
    else:
        print(tabela_pivot.to_string(index=False))

# identificar municipios-estado com dados incompletos intra-período (2006-2021)
municipios_estados_incompletos = df_painel1_final.groupby(['codigo', 'estado'])['ano'].nunique().reset_index()
municipios_estados_incompletos = municipios_estados_incompletos[municipios_estados_incompletos['ano'] < 16]  # 16 anos entre 2006 e 2021
print(f'\nNúmero de municípios-estados com informações incompletas para o período (2006-2021): {municipios_estados_incompletos.shape[0]}')
print(f'\nMunicípios-estados com informações incompletas para o período (2006-2021) (amostra):')
if municipios_estados_incompletos.empty:
    print('Nenhum município-estado com dados incompletos no período.')
else:
    print(municipios_estados_incompletos.head(20).to_string(index=False))
"""

# Liberação de memória
del tabela_pib_hab, df_pib_hab, tabela_bndes_total, df_bndes_total, df_pib_merge, df_bndes_merge, estado_map, df_painel1, total_desembolsos_ajustados_analise, total_desembolsos_ajustados_bndes, total_desembolsos_ajustados_bndes_999999, total_pib_real_analise, total_pib_real_ibge, municipios_estados_2023, total_populacao_analise, total_populacao_ibge, pib_lag1, pib_lag2, mask_usar_lag2, colunas_reordenadas, colunas_reordenadas_existentes, df_painel1_final, tabela_analise_final, df_painel1c, df_painel1c_final
gc.collect()

#_ ## CONCLUSÃO SOBRE PAINEL 1 e 1C ###
#_ Variável dependente, independente, lags, leads e controles criadas e consolidadas agrupadas por cada município-estado-ano.
#_ Período entre 2006-2021. Valores financeiros em MIL REAIS na base 2021 (inclui PIB em mil reais e PIB per capita em mil reais também).
#_ O código de município permaneceu como 6 dígitos.
#_ Painel naturalmente desbalanceado, nenhum município-estado perde dados ao longo da série histórica, mas existem casos de criação de municípios.
#_ ##-------------------------------###
# %% PAINEL DE DADOS 2 - MODELO 2 - Valor Adicionado Indústria real e desembolsos para setor indústria do BNDES (nível município-ano)
# ANÁLISE 2: MODELO PRINCIPAL - Evolução do Valor Adicionado Indústria real ao longo do tempo em respeito aos desembolsos do BNDES para setor indústria de cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)

# Carregar arquivos parquet para análise
tabela_pib_hab = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_pib_hab.parquet')
df_pib_hab = tabela_pib_hab.to_pandas()

tabela_bndes_total = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_industria.parquet')
df_bndes_total = tabela_bndes_total.to_pandas()

# Preparar df_pib_hab: selecionar colunas relevantes para merge
df_pib_merge = df_pib_hab[['codigo', 'municipio', 'estado', 'ano', 'populacao', 'pib_corrente', 'pib_real', 'va_industria_corrente', 'va_industria_real', 'va_agropecuaria_corrente', 'va_agropecuaria_real']].copy()

# Preparar df_bndes_total: renomear 'municipio_codigo' e 'ano' para compatibilidade
df_bndes_merge = df_bndes_total[['municipio_codigo', 'municipio', 'uf', 'ano', 'desembolsos_industria_corrente', 'desembolsos_industria_real']].copy()
df_bndes_merge.rename(columns={'municipio_codigo': 'codigo'}, inplace=True)

# Converter para string para garantir compatibilidade no merge
df_pib_merge['codigo'] = df_pib_merge['codigo'].astype('string')
df_pib_merge['ano'] = df_pib_merge['ano'].astype('string')
df_bndes_merge['codigo'] = df_bndes_merge['codigo'].astype('string')
df_bndes_merge['ano'] = df_bndes_merge['ano'].astype('string')

# Mapa de conversão: nome completo do estado -> sigla (para compatibilidade com PIB)
estado_map = {
    'RONDONIA': 'RO', 'ACRE': 'AC', 'AMAZONAS': 'AM', 'RORAIMA': 'RR', 'PARA': 'PA', 
    'AMAPA': 'AP', 'TOCANTINS': 'TO', 'MARANHAO': 'MA', 'PIAUI': 'PI', 'CEARA': 'CE', 
    'RIO GRANDE DO NORTE': 'RN', 'PARAIBA': 'PB', 'PERNAMBUCO': 'PE', 'ALAGOAS': 'AL', 
    'SERGIPE': 'SE', 'BAHIA': 'BA', 'MINAS GERAIS': 'MG', 'ESPIRITO SANTO': 'ES', 
    'RIO DE JANEIRO': 'RJ', 'SAO PAULO': 'SP', 'PARANA': 'PR', 'SANTA CATARINA': 'SC', 
    'RIO GRANDE DO SUL': 'RS', 'MATO GROSSO DO SUL': 'MS', 'MATO GROSSO': 'MT', 
    'GOIAS': 'GO', 'DISTRITO FEDERAL': 'DF'
}

# Converter UF do BNDES para sigla
df_bndes_merge['uf'] = df_bndes_merge['uf'].map(estado_map).astype('string')

# Extrair primeiros 6 dígitos do código BNDES (drop do dígito verificador) para compatibilidade com código do IBGE
df_bndes_merge['codigo'] = df_bndes_merge['codigo'].str[:6]

# Agregar desembolsos por Código + uf + Ano antes do merge
df_bndes_merge = df_bndes_merge.groupby(['codigo', 'uf', 'ano'], as_index=False)[
    ['municipio', 'desembolsos_industria_corrente', 'desembolsos_industria_real']
].sum()

# LEFT MERGE: manter todos os registros de df_pib_hab e unir com df_bndes_total quando houver correspondência
# Usar Código + Estado/uf + Ano como chaves de junção
df_painel2 = pd.merge(
    df_pib_merge,
    df_bndes_merge,
    left_on=['codigo', 'estado', 'ano'],
    right_on=['codigo', 'uf', 'ano'],
    how='left',
    suffixes=('_pib', '_bndes')
)

# Preencher valores de desembolso com zero onde não houver correspondência
df_painel2['desembolsos_industria_real'] = df_painel2['desembolsos_industria_real'].fillna(0)
df_painel2['desembolsos_industria_corrente'] = df_painel2['desembolsos_industria_corrente'].fillna(0)

# Consolidar coluna de município: usar municipio_pib quando disponível, caso contrário usar municipio_bndes
df_painel2['municipio'] = df_painel2['municipio_pib'].fillna(df_painel2['municipio_bndes'])

# Remover colunas redundantes
df_painel2.drop(columns=['uf', 'municipio_pib', 'municipio_bndes'], inplace=True)

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel2['ano'] = pd.to_numeric(df_painel2['ano'], errors='coerce')
df_painel2 = df_painel2.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável asinh_va_industria_real e variável dependente Y = delta asinh_va_industria_real em estrutura LONG
df_painel2['asinh_va_industria_real'] = np.arcsinh(df_painel2['va_industria_real'])
df_painel2['delta_asinh_va_industria_real'] = (df_painel2.groupby(['codigo', 'estado'])['asinh_va_industria_real'].diff())

# Calcular variável independente X = share_desembolso_real_pib_real_ano_anterior
# ! variável sem log, shift apenas no denominador (t-1, com fallback para t-2)
pib_lag1 = df_painel2.groupby(['codigo','estado'])['pib_real'].shift(1)
pib_lag2 = df_painel2.groupby(['codigo','estado'])['pib_real'].shift(2)

# ! Usar t-1 como padrão, mas quando t-1 for NaN ou zero, usar t-2
# Ocorre apenas em 1 caso, GUAMARE (RN) com PIB NEGATIVO em 2012
pib_lag = pib_lag1.copy()
mask_usar_lag2 = (pib_lag1.isna()) | (pib_lag1 == 0)
pib_lag[mask_usar_lag2] = pib_lag2[mask_usar_lag2]

df_painel2['share_desembolso_industria_real_ano_anterior'] = (df_painel2['desembolsos_industria_real'] / pib_lag)

# Calcular variáveis lag 1--3 da variável independente X
for lag in range(1, 4):
    df_painel2[f'share_desembolso_industria_real_ano_anterior_lag{lag}'] = df_painel2.groupby(['codigo','estado'])['share_desembolso_industria_real_ano_anterior'].shift(lag)

# Calcular variáveis de controle: log_populacao_lag1, log_pibpc_real_lag1 e share_industria_lag1 (ou seja, em t-1)
df_painel2['log_populacao'] = np.log(df_painel2['populacao'])
df_painel2['log_populacao_lag1'] = df_painel2.groupby(['codigo','estado'])['log_populacao'].shift(1)
df_painel2['pibpc_real'] = df_painel2['pib_real'] / df_painel2['populacao']
df_painel2['log_pibpc_real'] = np.log(df_painel2['pibpc_real'])
df_painel2['log_pibpc_real_lag1'] = df_painel2.groupby(['codigo','estado'])['log_pibpc_real'].shift(1)
df_painel2['share_industria'] = df_painel2['va_industria_real'] / df_painel2['pib_real']
df_painel2['share_industria_lag1'] = df_painel2.groupby(['codigo','estado'])['share_industria'].shift(1)
df_painel2['share_agropecuaria'] = df_painel2['va_agropecuaria_real'] / df_painel2['pib_real']
df_painel2['share_agropecuaria_lag1'] = df_painel2.groupby(['codigo','estado'])['share_agropecuaria'].shift(1)

# Reordenar colunas para facilitar conferência e análise
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'va_industria_corrente', 'va_industria_real','asinh_va_industria_real', 'delta_asinh_va_industria_real', 'desembolsos_corrente', 'desembolsos_real', 'desembolsos_industria_corrente', 'desembolsos_industria_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'share_desembolso_industria_real_ano_anterior', 'share_desembolso_industria_real_ano_anterior_lag1', 'share_desembolso_industria_real_ano_anterior_lag2', 'share_desembolso_industria_real_ano_anterior_lag3', 'log_populacao', 'log_populacao_lag1', 'pibpc_real', 'log_pibpc_real', 'log_pibpc_real_lag1', 'share_industria', 'share_industria_lag1', 'share_agropecuaria', 'share_agropecuaria_lag1']
colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel2.columns]
df_painel2 = df_painel2[colunas_reordenadas_existentes]

# MODELO COMPLEMENTAR - Robustez - Evolução do Valor Adicionado para Indústria real ao longo do tempo em respeito aos desembolsos do BNDES para o setor indústria de cada município (efeito regional) - valores constantes de 2023 (MIL REAIS)
# Inseridas leads da variável independente para análise de efeitos antecipados

# Carregar arquivos parquet para análise
df_painel2c = df_painel2.copy()  # Usar o painel2 já preparado como base para painel2c, que é o modelo complementar com leads

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel2c['ano'] = pd.to_numeric(df_painel2c['ano'], errors='coerce')
df_painel2c = df_painel2c.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável independente lead (xt+1) e (xt+2)
# ! variável sem log, shift no numerador para ano futuro
df_painel2c.loc[df_painel2c['pib_real'] <= 0, 'pib_real'] = np.nan  # Substituir valores de PIB real menores ou iguais a zero por NaN para evitar problemas de divisão e log
pib_t = df_painel2c['pib_real']  # PIB_t
pib_tp1 = df_painel2c.groupby(['codigo','estado'])['pib_real'].shift(-1) # PIB_{t+1}
desemb_tp1 = df_painel2c.groupby(['codigo','estado'])['desembolsos_industria_real'].shift(-1) # Desemb_{t+1}
desemb_tp2 = df_painel2c.groupby(['codigo','estado'])['desembolsos_industria_real'].shift(-2) # Desemb_{t+2}

df_painel2c['share_desembolso_industria_real_pib_real_ano_anterior_lead1'] = desemb_tp1 / pib_t
df_painel2c['share_desembolso_industria_real_pib_real_ano_anterior_lead2'] = desemb_tp2 / pib_tp1
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'va_industria_corrente', 'va_industria_real','asinh_va_industria_real', 'delta_asinh_va_industria_real', 'desembolsos_corrente', 'desembolsos_real', 'desembolsos_industria_corrente', 'desembolsos_industria_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'share_desembolso_industria_real_ano_anterior', 'share_desembolso_industria_real_ano_anterior_lag1', 'share_desembolso_industria_real_ano_anterior_lag2', 'share_desembolso_industria_real_ano_anterior_lag3', 'share_desembolso_industria_real_pib_real_ano_anterior_lead1', 'share_desembolso_industria_real_pib_real_ano_anterior_lead2', 'log_populacao', 'log_populacao_lag1', 'pibpc_real', 'log_pibpc_real', 'log_pibpc_real_lag1', 'share_industria', 'share_industria_lag1', 'share_agropecuaria', 'share_agropecuaria_lag1']

colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel2c.columns]
df_painel2c_final = df_painel2c[colunas_reordenadas_existentes]

# Verificação final do DataFrame PAINEL 2 de análise com tipos de dados
print(f'\nDataFrame Painel 2 (antes do drop de NA):')
print(f'Número de linhas e colunas: {df_painel2.shape}')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel2["ano"].min()} - {df_painel2["ano"].max()}')

# Realizar o drop de registros NA (devido a lags) e comparar quantidade de informações perdidas nas pontas
df_painel2_final = df_painel2.dropna()

print(f'\nDataFrame Painel 2 após drop de NA (devido a lags):')
print(f'Número de linhas eliminadas: {df_painel2.shape[0] - df_painel2_final.shape[0]}, equivalente a {((df_painel2.shape[0] - df_painel2_final.shape[0]) / df_painel2.shape[0]) * 100:.2f}% do total de linhas')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel2_final["ano"].min()} - {df_painel2_final["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel2_final)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel2.parquet', compression='snappy')

# Realizar o drop de registros NA (devido a lags) e comparar quantidade de informações perdidas nas pontas
df_painel2c_final = df_painel2c_final.dropna()

print(f'\nDataFrame Painel 2C após drop de NA (devido a lags):')
print(f'Número de linhas eliminadas: {df_painel2c_final.shape[0] - df_painel2.shape[0]}, equivalente a {((df_painel2c_final.shape[0] - df_painel2.shape[0]) / df_painel2.shape[0]) * 100:.2f}% do total de linhas do painel 1 original.')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel2c_final["ano"].min()} - {df_painel2c_final["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel2c_final)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel2c.parquet', compression='snappy')

print(f'Tipagens das colunas (2C):\n{df_painel2c_final.dtypes}')

# Liberação de memória
del colunas_reordenadas, colunas_reordenadas_existentes, tabela_analise_final, df_painel2c, df_painel2c_final, tabela_pib_hab, df_pib_hab, tabela_bndes_total, df_bndes_total, df_pib_merge, df_bndes_merge, estado_map, df_painel2, pib_lag1, pib_lag2, mask_usar_lag2, df_painel2_final
gc.collect()

#_ ## CONCLUSÃO SOBRE PAINEL 2 e 2C ###
#_ Variável dependente, independente, lags, leads e controles criadas e consolidadas agrupadas por cada município-estado-ano.
#_ Período entre 2006-2021. Valores financeiros em MIL REAIS na base 2021 (inclui PIB em mil reais e PIB per capita em mil reais também).
#_ O código de município permaneceu como 6 dígitos.
#_ ##-------------------------------###
# %% PAINEL DE DADOS 3 - MODELO 3 - Valor Adicionado Agropecuária real e desembolsos para setor agropecuário do BNDES (nível município-ano)
# ANÁLISE 3: MODELO PRINCIPAL - Evolução do Valor Adicionado Agropecuária real ao longo do tempo em respeito aos desembolsos do BNDES para setor agropecuário de cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)

# Carregar arquivos parquet para análise
tabela_pib_hab = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_pib_hab.parquet')
df_pib_hab = tabela_pib_hab.to_pandas()

tabela_bndes_total = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_agropecuaria.parquet')
df_bndes_total = tabela_bndes_total.to_pandas()

# Preparar df_pib_hab: selecionar colunas relevantes para merge
df_pib_merge = df_pib_hab[['codigo', 'municipio', 'estado', 'ano', 'populacao', 'pib_corrente', 'pib_real', 'va_industria_corrente', 'va_industria_real', 'va_agropecuaria_corrente', 'va_agropecuaria_real']].copy()

# Preparar df_bndes_total: renomear 'municipio_codigo' e 'ano' para compatibilidade
df_bndes_merge = df_bndes_total[['municipio_codigo', 'municipio', 'uf', 'ano', 'desembolsos_agropecuaria_corrente', 'desembolsos_agropecuaria_real']].copy()
df_bndes_merge.rename(columns={'municipio_codigo': 'codigo'}, inplace=True)

# Converter para string para garantir compatibilidade no merge
df_pib_merge['codigo'] = df_pib_merge['codigo'].astype('string')
df_pib_merge['ano'] = df_pib_merge['ano'].astype('string')
df_bndes_merge['codigo'] = df_bndes_merge['codigo'].astype('string')
df_bndes_merge['ano'] = df_bndes_merge['ano'].astype('string')

# Mapa de conversão: nome completo do estado -> sigla (para compatibilidade com PIB)
estado_map = {
    'RONDONIA': 'RO', 'ACRE': 'AC', 'AMAZONAS': 'AM', 'RORAIMA': 'RR', 'PARA': 'PA', 
    'AMAPA': 'AP', 'TOCANTINS': 'TO', 'MARANHAO': 'MA', 'PIAUI': 'PI', 'CEARA': 'CE', 
    'RIO GRANDE DO NORTE': 'RN', 'PARAIBA': 'PB', 'PERNAMBUCO': 'PE', 'ALAGOAS': 'AL', 
    'SERGIPE': 'SE', 'BAHIA': 'BA', 'MINAS GERAIS': 'MG', 'ESPIRITO SANTO': 'ES', 
    'RIO DE JANEIRO': 'RJ', 'SAO PAULO': 'SP', 'PARANA': 'PR', 'SANTA CATARINA': 'SC', 
    'RIO GRANDE DO SUL': 'RS', 'MATO GROSSO DO SUL': 'MS', 'MATO GROSSO': 'MT', 
    'GOIAS': 'GO', 'DISTRITO FEDERAL': 'DF'
}

# Converter UF do BNDES para sigla
df_bndes_merge['uf'] = df_bndes_merge['uf'].map(estado_map).astype('string')

# Extrair primeiros 6 dígitos do código BNDES (drop do dígito verificador) para compatibilidade com código do IBGE
df_bndes_merge['codigo'] = df_bndes_merge['codigo'].str[:6]

# Agregar desembolsos por Código + uf + Ano antes do merge
df_bndes_merge = df_bndes_merge.groupby(['codigo', 'uf', 'ano'], as_index=False)[
    ['municipio', 'desembolsos_agropecuaria_corrente', 'desembolsos_agropecuaria_real']
].sum()

# LEFT MERGE: manter todos os registros de df_pib_hab e unir com df_bndes_total quando houver correspondência
# Usar Código + Estado/uf + Ano como chaves de junção
df_painel3 = pd.merge(
    df_pib_merge,
    df_bndes_merge,
    left_on=['codigo', 'estado', 'ano'],
    right_on=['codigo', 'uf', 'ano'],
    how='left',
    suffixes=('_pib', '_bndes')
)

# Preencher valores de desembolso com zero onde não houver correspondência
df_painel3['desembolsos_agropecuaria_real'] = df_painel3['desembolsos_agropecuaria_real'].fillna(0)
df_painel3['desembolsos_agropecuaria_corrente'] = df_painel3['desembolsos_agropecuaria_corrente'].fillna(0)

# Consolidar coluna de município: usar municipio_pib quando disponível, caso contrário usar municipio_bndes
df_painel3['municipio'] = df_painel3['municipio_pib'].fillna(df_painel3['municipio_bndes'])

# Remover colunas redundantes
df_painel3.drop(columns=['uf', 'municipio_pib', 'municipio_bndes'], inplace=True)

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel3['ano'] = pd.to_numeric(df_painel3['ano'], errors='coerce')
df_painel3 = df_painel3.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável asinh_va_agropecuaria_real e variável dependente Y = delta asinh_va_agropecuaria_real em estrutura LONG
df_painel3['asinh_va_agropecuaria_real'] = np.arcsinh(df_painel3['va_agropecuaria_real'])
df_painel3['delta_asinh_va_agropecuaria_real'] = (df_painel3.groupby(['codigo', 'estado'])['asinh_va_agropecuaria_real'].diff())

# Calcular variável independente X = share_desembolso_real_pib_real_ano_anterior
# ! variável sem log, shift apenas no denominador (t-1, com fallback para t-2)
pib_lag1 = df_painel3.groupby(['codigo','estado'])['pib_real'].shift(1)
pib_lag2 = df_painel3.groupby(['codigo','estado'])['pib_real'].shift(2)

# ! Usar t-1 como padrão, mas quando t-1 for NaN ou zero, usar t-2
# Ocorre apenas em 1 caso, GUAMARE (RN) com PIB NEGATIVO em 2012
pib_lag = pib_lag1.copy()
mask_usar_lag2 = (pib_lag1.isna()) | (pib_lag1 == 0)
pib_lag[mask_usar_lag2] = pib_lag2[mask_usar_lag2]

df_painel3['share_desembolso_agropecuaria_real_ano_anterior'] = (df_painel3['desembolsos_agropecuaria_real'] / pib_lag)

# Calcular variáveis lag 1--3 da variável independente X
for lag in range(1, 4):
    df_painel3[f'share_desembolso_agropecuaria_real_ano_anterior_lag{lag}'] = df_painel3.groupby(['codigo','estado'])['share_desembolso_agropecuaria_real_ano_anterior'].shift(lag)

# Calcular variáveis de controle: log_populacao_lag1, log_pibpc_real_lag1 e share_industria_lag1 (ou seja, em t-1)
df_painel3['log_populacao'] = np.log(df_painel3['populacao'])
df_painel3['log_populacao_lag1'] = df_painel3.groupby(['codigo','estado'])['log_populacao'].shift(1)
df_painel3['pibpc_real'] = df_painel3['pib_real'] / df_painel3['populacao']
df_painel3['log_pibpc_real'] = np.log(df_painel3['pibpc_real'])
df_painel3['log_pibpc_real_lag1'] = df_painel3.groupby(['codigo','estado'])['log_pibpc_real'].shift(1)
df_painel3['share_industria'] = df_painel3['va_industria_real'] / df_painel3['pib_real']
df_painel3['share_industria_lag1'] = df_painel3.groupby(['codigo','estado'])['share_industria'].shift(1)
df_painel3['share_agropecuaria'] = df_painel3['va_agropecuaria_real'] / df_painel3['pib_real']
df_painel3['share_agropecuaria_lag1'] = df_painel3.groupby(['codigo','estado'])['share_agropecuaria'].shift(1)

# Reordenar colunas para facilitar conferência e análise
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'va_industria_corrente', 'va_industria_real', 'va_agropecuaria_corrente', 'va_agropecuaria_real', 'asinh_va_agropecuaria_real', 'delta_asinh_va_agropecuaria_real', 'desembolsos_corrente', 'desembolsos_real', 'desembolsos_agropecuaria_corrente', 'desembolsos_agropecuaria_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'share_desembolso_agropecuaria_real_ano_anterior', 'share_desembolso_agropecuaria_real_ano_anterior_lag1', 'share_desembolso_agropecuaria_real_ano_anterior_lag2', 'share_desembolso_agropecuaria_real_ano_anterior_lag3', 'log_populacao', 'log_populacao_lag1', 'pibpc_real', 'log_pibpc_real', 'log_pibpc_real_lag1', 'share_industria', 'share_industria_lag1', 'share_agropecuaria', 'share_agropecuaria_lag1']
colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel3.columns]
df_painel3 = df_painel3[colunas_reordenadas_existentes]

# MODELO COMPLEMENTAR - Robustez - Evolução do Valor Adicionado para Agropecuária real ao longo do tempo em respeito aos desembolsos do BNDES para o setor agropecuária de cada município (efeito regional) - valores constantes de 2023 (MIL REAIS)
# Inseridas leads da variável independente para análise de efeitos antecipados

# Carregar arquivos parquet para análise
df_painel3c = df_painel3.copy()  # Usar o painel3 já preparado como base para painel3c, que é o modelo complementar com leads

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel3c['ano'] = pd.to_numeric(df_painel3c['ano'], errors='coerce')
df_painel3c = df_painel3c.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável independente lead (xt+1) e (xt+2)
# ! variável sem log, shift no numerador para ano futuro
df_painel3c.loc[df_painel3c['pib_real'] <= 0, 'pib_real'] = np.nan  # Substituir valores de PIB real menores ou iguais a zero por NaN para evitar problemas de divisão e log
pib_t = df_painel3c['pib_real']  # PIB_t
pib_tp1 = df_painel3c.groupby(['codigo','estado'])['pib_real'].shift(-1) # PIB_{t+1}
desemb_tp1 = df_painel3c.groupby(['codigo','estado'])['desembolsos_agropecuaria_real'].shift(-1) # Desemb_{t+1}
desemb_tp2 = df_painel3c.groupby(['codigo','estado'])['desembolsos_agropecuaria_real'].shift(-2) # Desemb_{t+2}

df_painel3c['share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead1'] = desemb_tp1 / pib_t
df_painel3c['share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead2'] = desemb_tp2 / pib_tp1
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'va_agropecuaria_corrente', 'va_agropecuaria_real','asinh_va_agropecuaria_real', 'delta_asinh_va_agropecuaria_real', 'desembolsos_corrente', 'desembolsos_real', 'desembolsos_agropecuaria_corrente', 'desembolsos_agropecuaria_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'share_desembolso_agropecuaria_real_ano_anterior', 'share_desembolso_agropecuaria_real_ano_anterior_lag1', 'share_desembolso_agropecuaria_real_ano_anterior_lag2', 'share_desembolso_agropecuaria_real_ano_anterior_lag3', 'share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead1', 'share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead2', 'log_populacao', 'log_populacao_lag1', 'pibpc_real', 'log_pibpc_real', 'log_pibpc_real_lag1', 'share_agropecuaria', 'share_agropecuaria_lag1', 'share_industria', 'share_industria_lag1']

colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel3c.columns]
df_painel3c_final = df_painel3c[colunas_reordenadas_existentes]

# Verificação final do DataFrame de análise com tipos de dados
print(f'\nDataFrame Painel 3 (antes do drop de NA):')
print(f'Número de linhas e colunas: {df_painel3.shape}')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel3["ano"].min()} - {df_painel3["ano"].max()}')

# Realizar o drop de registros NA (devido a lags) e comparar quantidade de informações perdidas nas pontas
df_painel3_final = df_painel3.dropna()

print(f'\nDataFrame Painel 3 após drop de NA (devido a lags):')
print(f'Número de linhas eliminadas: {df_painel3.shape[0] - df_painel3_final.shape[0]}, equivalente a {((df_painel3.shape[0] - df_painel3_final.shape[0]) / df_painel3.shape[0]) * 100:.2f}% do total de linhas')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel3_final["ano"].min()} - {df_painel3_final["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel3_final)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel3.parquet', compression='snappy')

# Realizar o drop de registros NA (devido a lags) e comparar quantidade de informações perdidas nas pontas
df_painel3c_final = df_painel3c_final.dropna()

print(f'\nDataFrame Painel 3C após drop de NA (devido a lags e leads):')
print(f'Número de linhas eliminadas: {df_painel3c_final.shape[0] - df_painel3.shape[0]}, equivalente a {((df_painel3c_final.shape[0] - df_painel3.shape[0]) / df_painel3.shape[0]) * 100:.2f}% do total de linhas do painel 3c original.')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel3c_final["ano"].min()} - {df_painel3c_final["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel3c_final)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel3c.parquet', compression='snappy')

print(f'Tipagens das colunas (3C):\n{df_painel3c_final.dtypes}')

# Liberação de memória
del colunas_reordenadas, colunas_reordenadas_existentes, tabela_analise_final, tabela_pib_hab, df_pib_hab, tabela_bndes_total, df_bndes_total, df_pib_merge, df_bndes_merge, estado_map, df_painel3, pib_lag1, pib_lag2, mask_usar_lag2, df_painel3_final
gc.collect()

#_ ## CONCLUSÃO SOBRE PAINEL 3 ###
#_ Variável dependente, independente, lags, leads e controles criadas e consolidadas agrupadas por cada município-estado-ano.
#_ Período entre 2006-2021. Valores financeiros em MIL REAIS na base 2021 (inclui PIB em mil reais e PIB per capita em mil reais também).
#_ O código de município permaneceu como 6 dígitos.
#_ ##--------------------------###

# %% PAINEL DE DADOS 4 - MODELO 4 - PIB per capita real e desembolsos do BNDES (nível município-ano)
# ANÁLISE 4: MODELO PRINCIPAL - Evolução do PIB per capita real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)

# Remontar a base do painel1, de maneira independente
tabela_pib_hab = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_pib_hab.parquet')
df_pib_hab = tabela_pib_hab.to_pandas()

tabela_bndes_total = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes_total.parquet')
df_bndes_total = tabela_bndes_total.to_pandas()

# Preparar df_pib_hab: selecionar colunas relevantes para merge
df_pib_merge = df_pib_hab[['codigo', 'municipio', 'estado', 'ano', 'populacao', 'pib_corrente', 'pib_real', 'va_industria_corrente', 'va_industria_real', 'va_agropecuaria_corrente', 'va_agropecuaria_real']].copy()

# Preparar df_bndes_total: renomear 'municipio_codigo' e 'ano' para compatibilidade
df_bndes_merge = df_bndes_total[['municipio_codigo', 'municipio', 'uf', 'ano', 'desembolsos_corrente', 'desembolsos_real']].copy()
df_bndes_merge.rename(columns={'municipio_codigo': 'codigo'}, inplace=True)

# Converter para string para garantir compatibilidade no merge
df_pib_merge['codigo'] = df_pib_merge['codigo'].astype('string')
df_pib_merge['ano'] = df_pib_merge['ano'].astype('string')
df_bndes_merge['codigo'] = df_bndes_merge['codigo'].astype('string')
df_bndes_merge['ano'] = df_bndes_merge['ano'].astype('string')

# Mapa de conversão: nome completo do estado -> sigla (para compatibilidade com PIB)
estado_map = {
    'RONDONIA': 'RO', 'ACRE': 'AC', 'AMAZONAS': 'AM', 'RORAIMA': 'RR', 'PARA': 'PA', 
    'AMAPA': 'AP', 'TOCANTINS': 'TO', 'MARANHAO': 'MA', 'PIAUI': 'PI', 'CEARA': 'CE', 
    'RIO GRANDE DO NORTE': 'RN', 'PARAIBA': 'PB', 'PERNAMBUCO': 'PE', 'ALAGOAS': 'AL', 
    'SERGIPE': 'SE', 'BAHIA': 'BA', 'MINAS GERAIS': 'MG', 'ESPIRITO SANTO': 'ES', 
    'RIO DE JANEIRO': 'RJ', 'SAO PAULO': 'SP', 'PARANA': 'PR', 'SANTA CATARINA': 'SC', 
    'RIO GRANDE DO SUL': 'RS', 'MATO GROSSO DO SUL': 'MS', 'MATO GROSSO': 'MT', 
    'GOIAS': 'GO', 'DISTRITO FEDERAL': 'DF'
}

# Converter UF do BNDES para sigla
df_bndes_merge['uf'] = df_bndes_merge['uf'].map(estado_map).astype('string')

# Extrair primeiros 6 dígitos do código BNDES (drop do dígito verificador) para compatibilidade com código do IBGE
df_bndes_merge['codigo'] = df_bndes_merge['codigo'].str[:6]

# Agregar desembolsos por Código + uf + Ano antes do merge
df_bndes_merge = df_bndes_merge.groupby(['codigo', 'uf', 'ano'], as_index=False)[
    ['municipio', 'desembolsos_corrente', 'desembolsos_real']
].sum()

# LEFT MERGE: manter todos os registros de df_pib_hab e unir com df_bndes_total quando houver correspondência
# Usar Código + Estado/uf + Ano como chaves de junção
df_painel4 = pd.merge(
    df_pib_merge,
    df_bndes_merge,
    left_on=['codigo', 'estado', 'ano'],
    right_on=['codigo', 'uf', 'ano'],
    how='left',
    suffixes=('_pib', '_bndes')
)

# Preencher valores de desembolso com zero onde não houver correspondência
df_painel4['desembolsos_real'] = df_painel4['desembolsos_real'].fillna(0)
df_painel4['desembolsos_corrente'] = df_painel4['desembolsos_corrente'].fillna(0)

# Consolidar coluna de município: usar municipio_pib quando disponível, caso contrário usar municipio_bndes
df_painel4['municipio'] = df_painel4['municipio_pib'].fillna(df_painel4['municipio_bndes'])

# Remover colunas redundantes
df_painel4.drop(columns=['uf', 'municipio_pib', 'municipio_bndes'], inplace=True)

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel4['ano'] = pd.to_numeric(df_painel4['ano'], errors='coerce')
df_painel4 = df_painel4.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável log_pib_real e variável dependente Y = delta_log_pib_real em estrutura LONG
df_painel4['log_pib_real'] = np.log(df_painel4['pib_real'])
df_painel4['delta_log_pib_real'] = df_painel4.groupby(['codigo', 'estado'])['log_pib_real'].diff(1)

# Calcular variável pibpc_real, log_pibpc_real e variável dependente Y = delta_log_pibpc_real em estrutura LONG
df_painel4['pibpc_real'] = df_painel4['pib_real'] / df_painel4['populacao']
df_painel4['log_pibpc_real'] = np.log(df_painel4['pibpc_real'])
df_painel4['delta_log_pibpc_real'] = df_painel4.groupby(['codigo', 'estado'])['log_pibpc_real'].diff(1)

# Calcular variável independente X = share_desembolso_real_pib_real_ano_anterior
# ! variável sem log, shift apenas no denominador (t-1, com fallback para t-2)
pib_lag1 = df_painel4.groupby(['codigo','estado'])['pib_real'].shift(1)
pib_lag2 = df_painel4.groupby(['codigo','estado'])['pib_real'].shift(2)

# ! Usar t-1 como padrão, mas quando t-1 for NaN ou zero, usar t-2
# Ocorre apenas em 1 caso, GUAMARE (RN) com PIB NEGATIVO em 2012
pib_lag = pib_lag1.copy()
mask_usar_lag2 = (pib_lag1.isna()) | (pib_lag1 == 0)
pib_lag[mask_usar_lag2] = pib_lag2[mask_usar_lag2]

df_painel4['share_desembolso_real_pib_real_ano_anterior'] = (df_painel4['desembolsos_real'] / pib_lag)

# Calcular variáveis lag 1--3 da variável independente X
for lag in range(1, 4):
    df_painel4[f'share_desembolso_real_pib_real_ano_anterior_lag{lag}'] = df_painel4.groupby(['codigo','estado'])['share_desembolso_real_pib_real_ano_anterior'].shift(lag)

# Calcular variáveis de controle: log_populacao_lag1, log_pibpc_real_lag1 e share_industria_lag1 (ou seja, em t-1)
df_painel4['log_populacao'] = np.log(df_painel4['populacao'])
df_painel4['log_populacao_lag1'] = df_painel4.groupby(['codigo','estado'])['log_populacao'].shift(1)
df_painel4['pibpc_real'] = df_painel4['pib_real'] / df_painel4['populacao']
df_painel4['log_pibpc_real'] = np.log(df_painel4['pibpc_real'])
df_painel4['log_pibpc_real_lag1'] = df_painel4.groupby(['codigo','estado'])['log_pibpc_real'].shift(1)
df_painel4['share_industria'] = df_painel4['va_industria_real'] / df_painel4['pib_real']
df_painel4['share_industria_lag1'] = df_painel4.groupby(['codigo','estado'])['share_industria'].shift(1)
df_painel4['share_agropecuaria'] = df_painel4['va_agropecuaria_real'] / df_painel4['pib_real']
df_painel4['share_agropecuaria_lag1'] = df_painel4.groupby(['codigo','estado'])['share_agropecuaria'].shift(1)

    # Reordenar colunas para facilitar conferência e análise
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'log_pib_real', 'delta_log_pib_real', 'pibpc_real', 'log_pibpc_real', 'delta_log_pibpc_real', 'desembolsos_corrente', 'desembolsos_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'log_populacao', 'log_populacao_lag1', 'log_pibpc_real_lag1', 'va_industria_corrente', 'va_industria_real', 'share_industria', 'share_industria_lag1', 'va_agropecuaria_corrente', 'va_agropecuaria_real', 'share_agropecuaria', 'share_agropecuaria_lag1']
colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel4.columns]
df_painel4 = df_painel4[colunas_reordenadas_existentes]

# Reordenar colunas para facilitar conferência e análise
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'pibpc_real', 'log_pibpc_real', 'delta_log_pibpc_real', 'desembolsos_corrente', 'desembolsos_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'log_populacao', 'log_populacao_lag1', 'log_pibpc_real_lag1', 'va_industria_corrente', 'va_industria_real', 'share_industria', 'share_industria_lag1', 'va_agropecuaria_corrente', 'va_agropecuaria_real', 'share_agropecuaria', 'share_agropecuaria_lag1']
colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel4.columns]
df_painel4 = df_painel4[colunas_reordenadas_existentes]

# MODELO COMPLEMENTAR - Robustez - Evolução do PIB per capita real ao longo do tempo em respeito aos desembolsos do BNDES para cada município (efeito regional) - valores constantes de 2021 (MIL REAIS)
# Inseridas leads da variável independente para análise de efeitos antecipados

df_painel4c = df_painel4.copy()  # Usar o painel4 já preparado como base para painel4c, que é o modelo complementar com leads

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel4c['ano'] = pd.to_numeric(df_painel4c['ano'], errors='coerce')
df_painel4c = df_painel4c.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável independente lead (xt+1) e (xt+2)
# ! variável sem log, shift no numerador para ano futuro
df_painel4c.loc[df_painel4c['pib_real'] <= 0, 'pib_real'] = np.nan  # Substituir valores de PIB real menores ou iguais a zero por NaN para evitar problemas de divisão e log
pib_t = df_painel4c['pib_real']  # PIB_t
pib_tp1 = df_painel4c.groupby(['codigo','estado'])['pib_real'].shift(-1) # PIB_{t+1}
desemb_tp1 = df_painel4c.groupby(['codigo','estado'])['desembolsos_real'].shift(-1) # Desemb_{t+1}
desemb_tp2 = df_painel4c.groupby(['codigo','estado'])['desembolsos_real'].shift(-2) # Desemb_{t+2}

df_painel4c['share_desembolso_real_pib_real_ano_anterior_lead1'] = desemb_tp1 / pib_t
df_painel4c['share_desembolso_real_pib_real_ano_anterior_lead2'] = desemb_tp2 / pib_tp1
colunas_reordenadas = ['ano', 'codigo', 'municipio', 'estado', 'populacao', 'pib_corrente', 'pib_real', 'pibpc_real', 'log_pibpc_real', 'delta_log_pibpc_real', 'desembolsos_corrente', 'desembolsos_real', 'share_desembolso_real_pib_real_ano_anterior', 'share_desembolso_real_pib_real_ano_anterior_lag1', 'share_desembolso_real_pib_real_ano_anterior_lag2', 'share_desembolso_real_pib_real_ano_anterior_lag3', 'share_desembolso_real_pib_real_ano_anterior_lead1', 'share_desembolso_real_pib_real_ano_anterior_lead2', 'log_populacao', 'log_populacao_lag1', 'log_pibpc_real_lag1', 'va_industria_corrente', 'va_industria_real', 'share_industria', 'share_industria_lag1', 'va_agropecuaria_corrente', 'va_agropecuaria_real', 'share_agropecuaria', 'share_agropecuaria_lag1']

colunas_reordenadas_existentes = [col for col in colunas_reordenadas if col in df_painel4c.columns]
df_painel4c_final = df_painel4c[colunas_reordenadas_existentes]

# Verificação final do DataFrame de análise com tipos de dados
print(f'\nDataFrame Painel 4 (antes do drop de NA):')
print(f'Número de linhas e colunas: {df_painel4.shape}')
print(f'Menor e maior ano disponíveis: {df_painel4["ano"].min()} - {df_painel4["ano"].max()}')

# Realizar o drop de registros NA (devido a lags) e comparar quantidade de informações perdidas nas pontas
df_painel4_final = df_painel4.dropna()

print(f'\nDataFrame Painel 4 após drop de NA (devido a lags):')
print(f'Número de linhas eliminadas: {df_painel4_final.shape[0] - df_painel4.shape[0]}, equivalente a {((df_painel4.shape[0] - df_painel4_final.shape[0]) / df_painel4.shape[0]) * 100:.2f}% do total de linhas')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel4_final["ano"].min()} - {df_painel4_final["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel4_final)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel4.parquet', compression='snappy')

# Realizar o drop de registros NA (devido a lags) e comparar quantidade de informações perdidas nas pontas
df_painel4c_final = df_painel4c_final.dropna()

print(f'\nDataFrame Painel 4C após drop de NA (devido a lags e leads):')
print(f'Número de linhas eliminadas: {df_painel4c_final.shape[0] - df_painel4c.shape[0]}, equivalente a {((df_painel4c_final.shape[0] - df_painel4c.shape[0]) / df_painel4c.shape[0]) * 100:.2f}% do total de linhas')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel4c_final["ano"].min()} - {df_painel4c_final["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel4c_final)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel4c.parquet', compression='snappy')

print(f'Tipagens das colunas (4C):\n{df_painel4c_final.dtypes}')

# Liberação de memória
del pib_lag1, pib_lag2, mask_usar_lag2, colunas_reordenadas, colunas_reordenadas_existentes, df_painel4_final, tabela_analise_final, df_painel4c, df_painel4c_final
gc.collect()

#_ ## CONCLUSÃO SOBRE PAINEL 4 e 4C ###
#_ Variável dependente, independente, lags, leads e controles criadas e consolidadas agrupadas por cada município-estado-ano.
#_ Período entre 2006-2021. Valores financeiros em MIL REAIS na base 2021 (inclui PIB em mil reais e PIB per capita em mil reais também).
#_ O código de município permaneceu como 6 dígitos.
#_ ##-------------------------------###
# %%
