import pandas as pd
import numpy as np
import os
from pathlib import Path

# Configurar diretório
panels_dir = Path('inputs/panels')
parquet_files = sorted(panels_dir.glob('*.parquet'))

print("=" * 80)
print("ANÁLISE COMPARATIVA DOS ARQUIVOS PARQUET")
print("=" * 80)
print()

# Dicionário para armazenar dados dos painéis
paineis = {}
summary_data = []

# ============================================================================
# 1. CARREGANDO E INSPECIONANDO CADA ARQUIVO
# ============================================================================
for file in parquet_files:
    name = file.stem
    df = pd.read_parquet(file)
    paineis[name] = df
    
    # Criar chave única de município-estado
    if 'municipio' in df.columns and 'estado' in df.columns:
        df['mun_est'] = df['municipio'] + '_' + df['estado']
        unique_mun_est = df['mun_est'].nunique()
    else:
        unique_mun_est = None
    
    # Contar valores vazios
    total_records = len(df)
    null_counts = df.isnull().sum().sum()
    non_null_counts = total_records * len(df.columns) - null_counts
    
    summary_data.append({
        'arquivo': name,
        'total_registros': total_records,
        'total_colunas': len(df.columns),
        'municipios_unicos': df['municipio'].nunique() if 'municipio' in df.columns else None,
        'estados_unicos': df['estado'].nunique() if 'estado' in df.columns else None,
        'mun_est_unicos': unique_mun_est,
        'valores_vazios': null_counts,
        'valores_nao_vazios': non_null_counts,
        'shape': f"{total_records} x {len(df.columns)}"
    })

# Mostrar resumo bem formatado
print("1. RESUMO GERAL DOS PAINÉIS")
print("-" * 80)
summary_df = pd.DataFrame(summary_data)
for col in summary_df.columns:
    print(f"\n{col}:")
    for idx, row in summary_df.iterrows():
        print(f"  {row['arquivo']:12s}: {row[col]}")

# ============================================================================
# 2. VERIFICAR UNICIDADE DE MUNICÍPIOS-ESTADO
# ============================================================================
print("\n" + "=" * 80)
print("2. ANÁLISE DE MUNICÍPIOS-ESTADO ÚNICOS")
print("-" * 80)

mun_est_counts = {}
for name, df in paineis.items():
    if 'municipio' in df.columns and 'estado' in df.columns:
        mun_est = set(zip(df['municipio'], df['estado']))
        mun_est_counts[name] = len(mun_est)
        print(f"{name:12s}: {len(mun_est):4d} pares município-estado únicos")

# Comparar se são iguais
unique_counts = set(mun_est_counts.values())
if len(unique_counts) == 1:
    print(f"\n[OK] TODOS os arquivos têm o MESMO número de municípios-estado únicos: {unique_counts.pop()}")
else:
    print(f"\n[ALERTA] Os arquivos TÊM DIFERENTES números de municípios-estado")
    print(f"  Contagens: {unique_counts}")

# ============================================================================
# 3. COMPARAR VARIÁVEIS COM MESMO NOME
# ============================================================================
print("\n" + "=" * 80)
print("3. COMPARAÇÃO DE VARIÁVEIS POR ARQUIVO")
print("-" * 80)

all_columns = {}
for name, df in paineis.items():
    all_columns[name] = set(df.columns)
    print(f"\n{name:12s} ({len(df.columns)} colunas):")
    print(f"  Colunas: {', '.join(sorted(df.columns))}")

# Encontrar colunas em comum
common_cols = set.intersection(*all_columns.values())
print(f"\nColunas COMUNS a TODOS os arquivos ({len(common_cols)}):")
print(f"  {', '.join(sorted(common_cols))}")

# Colunas únicas
for name, cols in all_columns.items():
    unique = cols - set.intersection(*all_columns.values())
    if unique:
        print(f"\nColunas ÚNICAS em {name}:")
        print(f"  {', '.join(sorted(unique))}")

# ============================================================================
# 4. COMPARAR TOTAIS DE VARIÁVEIS NUMÉRICAS
# ============================================================================
print("\n" + "=" * 80)
print("4. TOTAIS DAS VARIÁVEIS NUMÉRICAS (SUM)")
print("-" * 80)

numeric_vars = {}
for name, df in paineis.items():
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_vars[name] = {}
    
    for col in numeric_cols:
        numeric_vars[name][col] = df[col].sum()

# Comparar por coluna
all_numeric_cols = set()
for cols in numeric_vars.values():
    all_numeric_cols.update(cols.keys())

for col in sorted(all_numeric_cols):
    print(f"\n{col}:")
    for name in sorted(paineis.keys()):
        if col in numeric_vars[name]:
            total = numeric_vars[name][col]
            print(f"  {name:12s}: {total:20,.2f}")
        else:
            print(f"  {name:12s}: (coluna não existe)")

# ============================================================================
# 5. ANÁLISE DE VALORES VAZIOS E NÃO VAZIOS
# ============================================================================
print("\n" + "=" * 80)
print("5. REGISTROS VAZIOS E NÃO VAZIOS")
print("-" * 80)

for name, df in paineis.items():
    total_cells = len(df) * len(df.columns)
    null_cells = df.isnull().sum().sum()
    non_null_cells = total_cells - null_cells
    
    null_pct = (null_cells / total_cells) * 100 if total_cells > 0 else 0
    non_null_pct = (non_null_cells / total_cells) * 100 if total_cells > 0 else 0
    
    print(f"\n{name}:")
    print(f"  Total de células: {total_cells:,}")
    print(f"  Células vazias (NaN/null): {null_cells:,} ({null_pct:.2f}%)")
    print(f"  Células não vazias: {non_null_cells:,} ({non_null_pct:.2f}%)")
    
    # Análise por coluna
    print(f"  Vazios por coluna:")
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            pct = (null_count / len(df)) * 100
            print(f"    {col:20s}: {null_count:5d} ({pct:6.2f}%)")

# ============================================================================
# 6. ANÁLISE DE SOBREPOSIÇÃO E DUPLICATAS
# ============================================================================
print("\n" + "=" * 80)
print("6. ANÁLISE DE SOBREPOSIÇÃO E DUPLICAÇÃO DE DADOS")
print("-" * 80)

# Identificar coluna de chave ou tempo
key_candidates = []
for name, df in paineis.items():
    cols = df.columns.tolist()
    
    # Procurar por colunas de data/tempo
    time_cols = [c for c in cols if any(x in c.lower() for x in ['ano', 'mes', 'data', 'date', 'year', 'month'])]
    # Procurar por IDs
    id_cols = [c for c in cols if any(x in c.lower() for x in ['municipio', 'estado', 'mun', 'est'])]
    
    key_candidates.append({'arquivo': name, 'time_cols': time_cols, 'id_cols': id_cols})

for kc in key_candidates:
    print(f"\n{kc['arquivo']}:")
    print(f"  Colunas de tempo: {kc['time_cols'] if kc['time_cols'] else '(nenhuma encontrada)'}")
    print(f"  Colunas de ID: {kc['id_cols'] if kc['id_cols'] else '(nenhuma encontrada)'}")

# Comparar se pares de arquivos têm dados duplicados
print("\n" + "-" * 80)
print("Verificando sobreposição entre pares de arquivos...")
print("-" * 80)

from itertools import combinations

min_comum_cases = []
for file1, file2 in combinations(sorted(paineis.keys()), 2):
    df1 = paineis[file1]
    df2 = paineis[file2]
    
    # Colunas comuns
    common_cols = list(set(df1.columns) & set(df2.columns))
    
    if 'municipio' in common_cols and 'estado' in common_cols:
        # Comparar pares município-estado
        mun_est_1 = set(zip(df1['municipio'], df1['estado']))
        mun_est_2 = set(zip(df2['municipio'], df2['estado']))
        
        intersection = mun_est_1 & mun_est_2
        union = mun_est_1 | mun_est_2
        jaccard = len(intersection) / len(union) if len(union) > 0 else 0
        
        min_comum_cases.append({
            'par': f"{file1} vs {file2}",
            'interseção': len(intersection),
            'união': len(union),
            'jaccard': jaccard,
            'sobreposição_%': jaccard * 100
        })

if min_comum_cases:
    print("\nÍndice de Jaccard (similaridade: 0=disjunto, 1=idêntico):")
    for case in min_comum_cases:
        print(f"\n{case['par']}")
        print(f"  Pares em comum: {case['interseção']}")
        print(f"  Pares únicos (união): {case['união']}")
        print(f"  Índice Jaccard: {case['jaccard']:.4f} ({case['sobreposição_%']:.2f}%)")

# ============================================================================
# 7. RECOMENDAÇÃO FINAL
# ============================================================================
print("\n" + "=" * 80)
print("7. ANÁLISE DE VIABILIDADE DE CONSOLIDAÇÃO")
print("-" * 80)

# Verificar se os padrões de nomes sugerem agrupamento
nome_grupos = {}
for name in paineis.keys():
    base = name.rstrip('c')  # Remove 'c' do final
    if base not in nome_grupos:
        nome_grupos[base] = []
    nome_grupos[base].append(name)

print("\nGrupos por padrão de nomes:")
for base, arquivos in sorted(nome_grupos.items()):
    print(f"  {base}: {arquivos}")

# Analisar se arquivos com 'c' no final têm dados diferentes
print("\nComparando arquivos complementares (com 'c'):")
for base, arquivos in sorted(nome_grupos.items()):
    if len(arquivos) == 2:
        principal = [a for a in arquivos if not a.endswith('c')][0]
        complementar = [a for a in arquivos if a.endswith('c')][0]
        
        df_principal = paineis[principal]
        df_complementar = paineis[complementar]
        
        # Comparar estrutura
        cols_principal = set(df_principal.columns)
        cols_complementar = set(df_complementar.columns)
        
        cols_apenas_principal = cols_principal - cols_complementar
        cols_apenas_complementar = cols_complementar - cols_principal
        
        print(f"\n{base}:")
        print(f"  {principal}: {len(df_principal)} registros, {len(cols_principal)} colunas")
        print(f"  {complementar}: {len(df_complementar)} registros, {len(cols_complementar)} colunas")
        
        if cols_apenas_principal:
            print(f"  Colunas apenas em {principal}: {', '.join(sorted(cols_apenas_principal))}")
        if cols_apenas_complementar:
            print(f"  Colunas apenas em {complementar}: {', '.join(sorted(cols_apenas_complementar))}")
        
        # Verificar se há linhas duplicadas
        if set(df_principal.columns) & set(df_complementar.columns):
            common_cols = list(set(df_principal.columns) & set(df_complementar.columns))
            
            # Comparar primeiro por chave principal (municipio, estado)
            if 'municipio' in common_cols and 'estado' in common_cols:
                principal_mun_est = set(zip(df_principal['municipio'], df_principal['estado']))
                complementar_mun_est = set(zip(df_complementar['municipio'], df_complementar['estado']))
                overlap = len(principal_mun_est & complementar_mun_est)
                print(f"  Sobreposição (município-estado): {overlap}/{max(len(principal_mun_est), len(complementar_mun_est))}")

# CONCLUSÃO
print("\n" + "=" * 80)
print("CONCLUSÃO E RECOMENDAÇÕES")
print("=" * 80)

all_dfs = list(paineis.values())
total_unique_registros = len(pd.concat(all_dfs, ignore_index=True))
total_if_simple_merge = sum(len(df) for df in all_dfs)

print(f"\nTotal de registros se concatenados: {total_if_simple_merge:,}")
print(f"Total de registros únicos (após remover exatos): {total_unique_registros:,}")

consolidacao_possivel = total_unique_registros < total_if_simple_merge * 0.9

if consolidacao_possivel:
    print(f"\n[OK] RECOMENDAÇÃO: Há potencial para consolidação!")
    print(f"  Redução possível: {(1 - total_unique_registros/total_if_simple_merge)*100:.1f}%")
else:
    print(f"\n[ALERTA] Os arquivos têm baixa sobreposição. Consolidação recomendada apenas se:")
    print(f"   - As variáveis forem complementares")
    print(f"   - Quiser um único arquivo de entrada")
    print(f"   - Estiver disposto a lidar com valores faltantes")

print("\n" + "=" * 80)
