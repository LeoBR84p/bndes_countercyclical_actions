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

# Drop linhas vazias
df_hab.dropna(inplace=True)

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

# APURAÇÃO DE DEFLATORES PARA PIB INDUSTRIAL + DESEMBOLSOS INDUSTRIAIS E PIB AGROPECUÁRIA + DESEMBOLSOS AGROPECUÁRIA
# Fonte 3: Tabela 10.1 - Valor adicionado bruto constante e corrente, segundo os grupos de atividades - 2000-2023
# https://ftp.ibge.gov.br/Contas_Nacionais/Sistema_de_Contas_Nacionais/2023/tabelas_xls/sinoticas/tab10_1.xls

def carregar_deflator_setor(setor, coluna_deflator, anos_min=2002, anos_max=2023):
    df_setor = pd.read_excel(Path(RAW_DATA_PATH) / 'tab10_1_deflator_pib_setor.xlsx', skiprows=4)
    df_setor.rename(columns={'Unnamed: 1': 'setor'}, inplace=True)
    df_setor = df_setor[df_setor['setor'] == setor].copy()

    df_setor.columns = df_setor.columns.astype(str)
    rename_dict = {'2000': '2000_corrente'}
    for ano in range(2001, 2022):
        unnamed_col = 2 * (ano - 1999)
        rename_dict[str(ano)] = f'{ano}_constante'
        rename_dict[f'Unnamed: {unnamed_col}'] = f'{ano}_corrente'
    df_setor.rename(columns=rename_dict, inplace=True)

    deflator_data = []
    for ano in range(2001, 2024):
        corrente_col = f'{ano}_corrente'
        constante_col = f'{ano}_constante'
        if corrente_col in df_setor.columns and constante_col in df_setor.columns:
            corrente = df_setor[corrente_col].values[0]
            constante = df_setor[constante_col].values[0]
            if pd.notna(corrente) and pd.notna(constante) and constante != 0:
                variacao = corrente / constante
                deflator_data.append({'ano': ano, 'variacao_deflator': variacao})

    df_processado = pd.DataFrame(deflator_data)
    df_processado = df_processado.sort_values('ano').reset_index(drop=True)
    df_processado['indice_encadeado'] = 100.0

    idx_2021 = df_processado[df_processado['ano'] == 2021].index
    if len(idx_2021) > 0:
        idx_2021 = idx_2021[0]
        for i in range(idx_2021 - 1, -1, -1):
            variacao = df_processado.at[i + 1, 'variacao_deflator']
            indice_seguinte = df_processado.at[i + 1, 'indice_encadeado']
            df_processado.at[i, 'indice_encadeado'] = indice_seguinte / variacao #type: ignore
        for i in range(idx_2021 + 1, len(df_processado)):
            variacao = df_processado.at[i - 1, 'variacao_deflator']
            indice_anterior = df_processado.at[i - 1, 'indice_encadeado']
            df_processado.at[i, 'indice_encadeado'] = indice_anterior * variacao #type: ignore

    df_final = df_processado[['ano', 'indice_encadeado']].copy()
    df_final.rename(columns={'indice_encadeado': coluna_deflator}, inplace=True)
    df_final = df_final[(df_final['ano'] >= anos_min) & (df_final['ano'] <= anos_max)].copy()
    return df_final

# Importar deflatores por setor
df_deflator_pib_industria = carregar_deflator_setor(
    setor='Indústria',
    coluna_deflator='deflator_pib_industria_2021'
)
df_deflator_pib_agropecuaria = carregar_deflator_setor(
    setor='Agropecuária',
    coluna_deflator='deflator_pib_agropecuaria_2021'
)

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
del df_deflator_pib, df_deflator_pib_industria, df_deflator_pib_agropecuaria, df_deflatores, tabela_deflatores
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

# FILTRO DE ANOMALIAS: Substituir pib_corrente negativo por vazio (apenas 1 caso - GUAMARÉ 2012)
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
#_ Havia um caso no município de GUAMARE (RN) em 2002 onde o valor de PIB corrente estava negativo. Este valor foi substituído por vazio (NaN) para evitar distorções nas análises futuras.
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

#_ ## CONCLUSÃO DOS AJUSTES MANUAIS E CORRESPONDÊNCIA ENTRE AS BASES ###
#_ Após ajustes manuais, o número de correspondências entre as bases é 100%
#_ Em ambas as bases, existem 5570 municípios únicos e 27 Estados com nomes únicos de correspondência.
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
df_final['va_industria_real_pib'] = (df_final['va_industria_corrente'] * 100) / df_final['deflator_pib_2021']
df_final['va_agropecuaria_real_pib'] = (df_final['va_agropecuaria_corrente'] * 100) / df_final['deflator_pib_2021']

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
del df_pib, mask_neg, pib_negativo, pib_municipios, hab_municipios, df_hab, apenas_hab, apenas_pib, match, mapeamento_hab, mapeamento_condicional, anos_colunas, colunas_id, anos_existentes, df_hab_long, populacao_invalida, df_final, colunas_principais, pib_original_2023, pib_final_2023, pop_original_2023, pop_final_2023, tabela_deflatores, df_deflatores_temp, pib_real_invalido, tabela_final
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

# Converter ano para numérico
df_bndes['ano'] = pd.to_numeric(df_bndes['ano'], errors='coerce')

# Carregar tabela de deflatores
tabela_deflatores = pq.read_table(Path(PROCESSED_DATA_PATH) / 'tabela_deflatores.parquet')
df_deflatores_temp = tabela_deflatores.to_pandas()

# CRIAR AGREGAÇÕES POR SETOR ANTES DO MERGE
# 1. Desembolsos totais (todos os setores)
df_bndes_total = df_bndes.groupby(['ano', 'municipio_codigo', 'municipio', 'uf'], as_index=False).agg({'desembolsos_corrente': 'sum'})
df_bndes_total.rename(columns={'desembolsos_corrente': 'desembolsos_total_corrente'}, inplace=True)

# 2. Desembolsos industriais (agregado de 4 subsetores)
df_bndes_industria = df_bndes[df_bndes['setor_cnae'].isin(['INDÚSTRIA DE TRANSFORMAÇÃO', 'INDÚSTRIA EXTRATIVA', 'INDÚSTRIA DE UTILIDADES PÚBLICAS', 'INDÚSTRIA DE CONSTRUÇÃO'])].groupby(['ano', 'municipio_codigo', 'municipio', 'uf'], as_index=False).agg({'desembolsos_corrente': 'sum'})
df_bndes_industria.rename(columns={'desembolsos_corrente': 'desembolsos_industria_corrente'}, inplace=True)

# 3. Desembolsos agropecuários
df_bndes_agropecuaria = df_bndes[df_bndes['setor_cnae'].isin(['AGROPECUÁRIA'])].groupby(['ano', 'municipio_codigo', 'municipio', 'uf'], as_index=False).agg({'desembolsos_corrente': 'sum'})
df_bndes_agropecuaria.rename(columns={'desembolsos_corrente': 'desembolsos_agropecuaria_corrente'}, inplace=True)

# UNIR TODAS AS AGREGAÇÕES EM UM ÚNICO DATAFRAME
df_bndes_consolidado = df_bndes_total.copy()

# Merge com indústria (LEFT para manter todos os registros de total, preenchendo com 0 quando não houver desembolso industrial)
df_bndes_consolidado = pd.merge(
    df_bndes_consolidado,
    df_bndes_industria,
    on=['ano', 'municipio_codigo', 'municipio', 'uf'],
    how='left'
)

# Merge com agropecuária (LEFT para manter todos os registros, preenchendo com 0 quando não houver desembolso agropecuário)
df_bndes_consolidado = pd.merge(
    df_bndes_consolidado,
    df_bndes_agropecuaria,
    on=['ano', 'municipio_codigo', 'municipio', 'uf'],
    how='left'
)

# Preencher NaN com 0 (significa que houve atividade BNDES no município, mas não naquele setor específico)
df_bndes_consolidado['desembolsos_industria_corrente'] = df_bndes_consolidado['desembolsos_industria_corrente'].fillna(0)
df_bndes_consolidado['desembolsos_agropecuaria_corrente'] = df_bndes_consolidado['desembolsos_agropecuaria_corrente'].fillna(0)

# APLICAR DEFLATORES
df_bndes_consolidado = pd.merge(
    df_bndes_consolidado,
    df_deflatores_temp,
    on='ano',
    how='left'
)

# Calcular valores reais com deflatores específicos
df_bndes_consolidado['desembolsos_real_pib'] = (df_bndes_consolidado['desembolsos_total_corrente'] * 100) / df_bndes_consolidado['deflator_pib_2021']
df_bndes_consolidado['desembolsos_industria_real_pib'] = (df_bndes_consolidado['desembolsos_industria_corrente'] * 100) / df_bndes_consolidado['deflator_pib_2021']
df_bndes_consolidado['desembolsos_agropecuaria_real_pib'] = (df_bndes_consolidado['desembolsos_agropecuaria_corrente'] * 100) / df_bndes_consolidado['deflator_pib_2021']
df_bndes_consolidado['desembolsos_industria_real_va'] = (df_bndes_consolidado['desembolsos_industria_corrente'] * 100) / df_bndes_consolidado['deflator_pib_industria_2021']
df_bndes_consolidado['desembolsos_agropecuaria_real_va'] = (df_bndes_consolidado['desembolsos_agropecuaria_corrente'] * 100) / df_bndes_consolidado['deflator_pib_agropecuaria_2021']

# Remover colunas de deflatores e renomear
df_bndes_consolidado = df_bndes_consolidado.drop(columns=['deflator_pib_2021', 'deflator_pib_industria_2021', 'deflator_pib_agropecuaria_2021'])
df_bndes_consolidado = df_bndes_consolidado.rename(columns={'desembolsos_total_corrente': 'desembolsos_corrente'})

# Verificação dos dados consolidados
print(f'\nDataFrame consolidado de desembolsos do BNDES após deflação:')
print(f'Número de linhas e colunas: {df_bndes_consolidado.shape}')
print(f'Tipagens das colunas:\n{df_bndes_consolidado.dtypes}')
print(f'\nEstatísticas descritivas:')
print(df_bndes_consolidado[['desembolsos_real_pib', 'desembolsos_industria_real_pib', 'desembolsos_industria_real_va', 'desembolsos_agropecuaria_real_pib', 'desembolsos_agropecuaria_real_va']].describe())

# Gerar arquivos parquet 
tabela_bndes_consolidado = pa.Table.from_pandas(df_bndes_consolidado)
pq.write_table(tabela_bndes_consolidado, Path(PROCESSED_DATA_PATH) / 'base_bndes.parquet', compression='snappy')

# Liberação de memória
del df_bndes, linhas_reclassificadas, colunas_para_drop, colunas_existentes_para_drop, df_bndes_total, df_bndes_industria, df_bndes_agropecuaria, tabela_deflatores, df_deflatores_temp, df_bndes_consolidado, tabela_bndes_consolidado
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
# %% PAINEL DE DADOS (nível município-ano)
# PAINEL DE DADOS PARA ANÁLISE (nível município-ano)

# Carregar arquivos parquet para análise
tabela_pib_hab = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_pib_hab.parquet')
df_pib_hab = tabela_pib_hab.to_pandas()

tabela_bndes = pq.read_table(Path(PROCESSED_DATA_PATH) / 'base_bndes.parquet')
df_bndes = tabela_bndes.to_pandas()

# Preparar df_pib_hab: selecionar colunas relevantes para merge
df_pib_merge = df_pib_hab[['codigo', 'municipio', 'estado', 'ano', 'populacao', 'pib_corrente', 'pib_real', 'va_industria_corrente', 'va_industria_real', 'va_industria_real_pib', 'va_agropecuaria_corrente', 'va_agropecuaria_real', 'va_agropecuaria_real_pib']].copy()

# Preparar df_bndes: renomear 'municipio_codigo' e 'ano' para compatibilidade
df_bndes_merge = df_bndes[['municipio_codigo', 'municipio', 'uf', 'ano', 'desembolsos_corrente', 'desembolsos_real_pib', 'desembolsos_industria_corrente', 'desembolsos_industria_real_pib', 'desembolsos_industria_real_va', 'desembolsos_agropecuaria_corrente', 'desembolsos_agropecuaria_real_pib', 'desembolsos_agropecuaria_real_va']].copy()
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
df_bndes_merge = df_bndes_merge.groupby(['codigo', 'uf', 'ano'], as_index=False).agg({
    'municipio': 'first',
    'desembolsos_corrente': 'sum',
    'desembolsos_real_pib': 'sum',
    'desembolsos_industria_corrente': 'sum',
    'desembolsos_industria_real_pib': 'sum',
    'desembolsos_industria_real_va': 'sum',
    'desembolsos_agropecuaria_corrente': 'sum',
    'desembolsos_agropecuaria_real_pib': 'sum',
    'desembolsos_agropecuaria_real_va': 'sum'
})

# LEFT MERGE: manter todos os registros de df_pib_hab e unir com df_bndes quando houver correspondência
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
df_painel1['desembolsos_real_pib'] = df_painel1['desembolsos_real_pib'].fillna(0)
df_painel1['desembolsos_corrente'] = df_painel1['desembolsos_corrente'].fillna(0)
df_painel1['desembolsos_industria_real_pib'] = df_painel1['desembolsos_industria_real_pib'].fillna(0)
df_painel1['desembolsos_industria_corrente'] = df_painel1['desembolsos_industria_corrente'].fillna(0)
df_painel1['desembolsos_industria_real_va'] = df_painel1['desembolsos_industria_real_va'].fillna(0)
df_painel1['desembolsos_agropecuaria_real_pib'] = df_painel1['desembolsos_agropecuaria_real_pib'].fillna(0)
df_painel1['desembolsos_agropecuaria_real_va'] = df_painel1['desembolsos_agropecuaria_real_va'].fillna(0)
df_painel1['desembolsos_agropecuaria_corrente'] = df_painel1['desembolsos_agropecuaria_corrente'].fillna(0)

# Consolidar coluna de município: usar municipio_pib quando disponível, caso contrário usar municipio_bndes
df_painel1['municipio'] = df_painel1['municipio_pib'].fillna(df_painel1['municipio_bndes'])

# Remover colunas redundantes
df_painel1.drop(columns=['uf', 'municipio_pib', 'municipio_bndes'], inplace=True)

# verificar se o total de desembolsos ajustados no DataFrame de análise é igual ao total de desembolsos ajustados na base do BNDES
total_desembolsos_ajustados_analise = df_painel1['desembolsos_real_pib'].sum()
total_desembolsos_ajustados_bndes = df_bndes_merge['desembolsos_real_pib'].sum()
total_desembolsos_ajustados_bndes_999999 = df_bndes_merge[df_bndes_merge["codigo"] == "999999"]["desembolsos_real_pib"].sum()

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

# Garantir ordenação por código, estado e ano para cálculo correto de diferenças
df_painel1['ano'] = pd.to_numeric(df_painel1['ano'], errors='coerce')
df_painel1 = df_painel1.sort_values(by=['codigo', 'estado', 'ano'])

# Calcular variável em função do PIB, além de suas diferenças (delta) ano a ano
df_painel1['log_pib_real'] = np.log(df_painel1['pib_real'])
df_painel1['asinh_va_industria_real_pib'] = np.arcsinh(df_painel1['va_industria_real_pib'])
df_painel1['asinh_va_agropecuaria_real_pib'] = np.arcsinh(df_painel1['va_agropecuaria_real_pib'])
df_painel1['delta_log_pib_real'] = df_painel1.groupby(['codigo', 'estado'])['log_pib_real'].diff(1)
df_painel1['delta_asinh_va_industria_real_pib'] = (df_painel1.groupby(['codigo', 'estado'])['asinh_va_industria_real_pib'].diff())
df_painel1['delta_asinh_va_agropecuaria_real_pib'] = (df_painel1.groupby(['codigo', 'estado'])['asinh_va_agropecuaria_real_pib'].diff())
df_painel1['pibpc_real'] = df_painel1['pib_real'] / df_painel1['populacao']
df_painel1['log_pibpc_real'] = np.log(df_painel1['pibpc_real'])
df_painel1['delta_log_pibpc_real'] = df_painel1.groupby(['codigo', 'estado'])['log_pibpc_real'].diff(1)

# Calcular variável em função do VA de cada setor, além de suas diferenças (delta) ano a ano
df_painel1['asinh_va_industria_real'] = np.arcsinh(df_painel1['va_industria_real'])
df_painel1['asinh_va_agropecuaria_real'] = np.arcsinh(df_painel1['va_agropecuaria_real'])
df_painel1['delta_asinh_va_industria_real'] = (df_painel1.groupby(['codigo', 'estado'])['asinh_va_industria_real'].diff())
df_painel1['delta_asinh_va_agropecuaria_real'] = (df_painel1.groupby(['codigo', 'estado'])['asinh_va_agropecuaria_real'].diff())

# Calcular variáveis lag 1--3 das variáveis acima
for lag in range(1, 4):
    df_painel1[f'delta_asinh_va_industria_real_lag{lag}'] = df_painel1.groupby(['codigo','estado'])['delta_asinh_va_industria_real'].shift(lag)
    df_painel1[f'delta_asinh_va_agropecuaria_real_lag{lag}'] = df_painel1.groupby(['codigo','estado'])['delta_asinh_va_agropecuaria_real'].shift(lag)

# Calcular pib_real com lag
pib_lag1 = df_painel1.groupby(['codigo','estado'])['pib_real'].shift(1)
pib_lag2 = df_painel1.groupby(['codigo','estado'])['pib_real'].shift(2)

# Calcular delta_log_pib_real e log_pib_real com lag
delta_log_pib_real_lag1 = df_painel1.groupby(['codigo','estado'])['delta_log_pib_real'].shift(1)
delta_log_pib_real_lag2 = df_painel1.groupby(['codigo','estado'])['delta_log_pib_real'].shift(2)
log_pibpc_real_lag1 = df_painel1.groupby(['codigo','estado'])['log_pibpc_real'].shift(1)

# Adicionar lag1 e lag2 do delta_log_pib_real ao dataframe
df_painel1['delta_log_pib_real_lag1'] = delta_log_pib_real_lag1
df_painel1['delta_log_pib_real_lag2'] = delta_log_pib_real_lag2

# ! Usar t-1 como padrão, mas quando t-1 for NaN ou zero, usar t-2 (ocorre apenas em 1 caso, GUAMARE (RN) com PIB NEGATIVO em 2012)
pib_lag = pib_lag1.copy()
mask_usar_lag2 = (pib_lag1.isna()) | (pib_lag1 == 0)
pib_lag[mask_usar_lag2] = pib_lag2[mask_usar_lag2]

# Calcular variáveis de share_desembolso_real em relação aos pib_real_ano_anterior
df_painel1['share_desembolso_real_pib_real_ano_anterior'] = (df_painel1['desembolsos_real_pib'] / pib_lag)
df_painel1['share_desembolso_industria_real_ano_anterior'] = (df_painel1['desembolsos_industria_real_pib'] / pib_lag)
df_painel1['share_desembolso_agropecuaria_real_ano_anterior'] = (df_painel1['desembolsos_agropecuaria_real_pib'] / pib_lag)
df_painel1['share_desembolso_pc_real_pib_real_ano_anterior'] = (df_painel1['desembolsos_real_pib'] / df_painel1['populacao'] / pib_lag) # desembolso per capita

# Calcular variáveis lag 1--3 das variáveis acima
for lag in range(1, 4):
    df_painel1[f'share_desembolso_real_pib_real_ano_anterior_lag{lag}'] = df_painel1.groupby(['codigo','estado'])['share_desembolso_real_pib_real_ano_anterior'].shift(lag)
    df_painel1[f'share_desembolso_industria_real_ano_anterior_lag{lag}'] = df_painel1.groupby(['codigo','estado'])['share_desembolso_industria_real_ano_anterior'].shift(lag)
    df_painel1[f'share_desembolso_agropecuaria_real_ano_anterior_lag{lag}'] = df_painel1.groupby(['codigo','estado'])['share_desembolso_agropecuaria_real_ano_anterior'].shift(lag)
    df_painel1[f'share_desembolso_pc_real_pib_real_ano_anterior_lag{lag}'] = df_painel1.groupby(['codigo','estado'])['share_desembolso_pc_real_pib_real_ano_anterior'].shift(lag)

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

# Calcular variável independente lead (Xt+1) e (Xt+2)
# ! variável sem log, shift no numerador para ano futuro
df_painel1.loc[df_painel1['pib_real'] <= 0, 'pib_real'] = np.nan  # Substituir valores de PIB real menores ou iguais a zero por NaN para evitar problemas de divisão e log
pib_t = df_painel1['pib_real']  # PIB_t
pib_tp1 = df_painel1.groupby(['codigo','estado'])['pib_real'].shift(-1) # PIB_{t+1}
desemb_tp1 = df_painel1.groupby(['codigo','estado'])['desembolsos_real_pib'].shift(-1) # Desemb_{t+1}
desemb_tp2 = df_painel1.groupby(['codigo','estado'])['desembolsos_real_pib'].shift(-2) # Desemb_{t+2}
desemb_tp1_ind = df_painel1.groupby(['codigo','estado'])['desembolsos_industria_real_pib'].shift(-1) # Desemb_{t+1}
desemb_tp2_ind = df_painel1.groupby(['codigo','estado'])['desembolsos_industria_real_pib'].shift(-2) # Desemb_{t+2}
desemb_tp1_agro = df_painel1.groupby(['codigo','estado'])['desembolsos_agropecuaria_real_pib'].shift(-1) # Desemb_{t+1}
desemb_tp2_agro = df_painel1.groupby(['codigo','estado'])['desembolsos_agropecuaria_real_pib'].shift(-2) # Desemb_{t+2}

df_painel1['share_desembolso_real_pib_real_ano_anterior_lead1'] = desemb_tp1 / pib_t
df_painel1['share_desembolso_real_pib_real_ano_anterior_lead2'] = desemb_tp2 / pib_tp1
df_painel1['share_desembolso_industria_real_pib_real_ano_anterior_lead1'] = desemb_tp1_ind / pib_t
df_painel1['share_desembolso_industria_real_pib_real_ano_anterior_lead2'] = desemb_tp2_ind / pib_tp1
df_painel1['share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead1'] = desemb_tp1_agro / pib_t
df_painel1['share_desembolso_agropecuaria_real_pib_real_ano_anterior_lead2'] = desemb_tp2_agro / pib_tp1

# Verificação final do DataFrame de análise com tipos de dados
print(f'\nDataFrame Painel (sem drop de NA):')
print(f'Número de linhas e colunas: {df_painel1.shape}')
print(f'Menor e maior ano disponíveis após drop de NA: {df_painel1["ano"].min()} - {df_painel1["ano"].max()}')

tabela_analise_final = pa.Table.from_pandas(df_painel1)
pq.write_table(tabela_analise_final, Path(FINAL_DATA_PATH) / 'painel1.parquet', compression='snappy')

#_ ## CONCLUSÃO SOBRE PAINEL ###
#_ Variável dependente, independente, lags, leads e controles criadas e consolidadas agrupadas por cada município-estado-ano.
#_ Período entre 2006-2021. Valores financeiros em MIL REAIS na base 2021 (inclui PIB em mil reais e PIB per capita em mil reais também).
#_ O código de município permaneceu como 6 dígitos.
#_ Painel naturalmente desbalanceado, nenhum município-estado perde dados ao longo da série histórica, mas existem casos de criação de municípios.
#_ ##-------------------------------###

# Liberação de memória
del tabela_pib_hab, df_pib_hab, tabela_bndes, df_bndes, df_pib_merge, df_bndes_merge, estado_map, df_painel1, total_desembolsos_ajustados_analise, total_desembolsos_ajustados_bndes, total_desembolsos_ajustados_bndes_999999, total_pib_real_analise, total_pib_real_ibge, pib_lag1, pib_lag2, pib_lag, mask_usar_lag2, municipios_anos, municipios_anos_completo, municipios_incompletos, pib_t, pib_tp1, desemb_tp1, desemb_tp2, desemb_tp1_ind, desemb_tp2_ind, desemb_tp1_agro, desemb_tp2_agro, tabela_analise_final
gc.collect()
# %%
