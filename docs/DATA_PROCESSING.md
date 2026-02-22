# Processamento de Dados - TCC

## 🔍 Período de Análise
**2002–2021** (dados de 2022 e 2023 incompletos excluídos)

## 📌 Bases Originais

### 1️⃣ **Population (DATASUS)**
- **Arquivo:** `POP_MUNICIPIOS.csv`
- **Período:** 2002–2023 (filtrado para 2002–2021)
- **Formato:** Wide (anos em colunas)
- **Conteúdo:** População residente por município
- **Código Município** 6 dígitos (sem DV)
- **Registros:** 5.570 municípios únicos × 20 anos = 111.400 linhas após melt

### 2️⃣ **PIB Municipal (IBGE)**
- **Arquivo:** `PIB2002-2023.csv`
- **Período:** 2002–2023 (filtrado para 2002–2021)
- **Formato:** Long (uma linha por município-ano)
- **Conteúdo:** 
  - PIB a preços correntes (mil reais)
  - Valor adicionado: agropecuária, indústria, serviços, administração (preços correntes)
- **Registros:** ~122.500 linhas após limpeza
- **Tratamentos:** 
  - Extração de Estado das siglas entre parênteses
  - Normalização de nomes de municípios (remoção de acentos, padronização de preposições)
  - Ajuste manual de grafias inconsistentes (ex: Dona Eusebia/Euzebia, Itapage/Itapaje)
  - Drop de municípios sem dados numéricos

### 3️⃣ **Deflatores (IBGE)**
- **Tabela 06:** PIB com deflator encadeado (2021 = 100)
- **Tabela 10.1:** Valor adicionado por setor com deflator encadeado (2021 = 100)
- **Método:** Índice encadeado acumulado retroativo (de 2021 até 2002)
- **Resultado:** Deflatores `deflator_pib_2021` e `deflator_pib_industria_2021`

### 4️⃣ **Desembolsos BNDES**
- **Arquivo:** `desembolsos_mensais.csv`
- **Período:** 2002–2023 (filtrado para 2002–2021)
- **Formato:** Mensais (agregados em anuais)
- **Conteúdo:** Desembolsos por município, atividade econômica, forma de apoio
- **Código Município:** 7 dígitos (com DV)
- **Tratamentos:**
  - Conversão de reais para mil reais (÷1000)
  - Agrupamento monthly → annual
  - **Filtro de código especial:** `999999` (desembolsos não-localizáveis: ~743 bilhões mil reais em valores de 2021) removido
  - Conversão de 7→6 dígitos para match com PIB
  - Conversão de nomes de estado (ex: RONDONIA→RO)

---

## 📌 Bases Transformadas

### **base_pib_hab.parquet**
- **Linhas:** 122.466 (município-ano)
- **Período:** 2002–2021
- **Colunas:**
  - Código, Município, Estado, Ano
  - População
  - PIB_corrente, PIB_real, PIB_per_capita_real
  - va_industria_corrente, va_industria_real, va_industria_per_capita_real
- **Valores:** Mil reais, preços constantes 2021
- **Merge:** População (DATASUS) + PIB Municipal (IBGE)

### **base_bndes_total.parquet**
- **Linhas:** ~60.000 (município-ano com desembolsos > 0)
- **Período:** 2002–2021
- **Colunas:**
  - municipio_codigo (6 dígitos), municipio, uf, ano
  - desembolsos_mil_reais, desembolsos_mil_reais_ajustados
- **Valores:** Mil reais, preços constantes 2021 (ajustado com deflator PIB geral)
- **Dados:** Todos os setores, excluindo código 999999

### **base_bndes_industria.parquet**
- **Linhas:** ~30.000 (município-ano com desembolsos indústria > 0)
- **Período:** 2002–2021
- **Colunas:** Mesmas que total
- **Valores:** Mil reais, preços constantes 2021 (ajustado com deflator PIB industrial)
- **Filtro:** Apenas "Indústria de Transformação" + "Indústria Extrativa"

---

## 📌 Painéis de Análise

> **Colunas comuns:** Código, Município, Estado, Ano, População, PIB_real, PIB_per_capita_real, desembolsos_mil_reais_ajustados, log_PIB_real, log_PIB_per_capita_real, log1p_desembolsos_mil_reais_ajustados

> **Tratamento por log:** aplicado log(x) e log1p(x) para valores positivos de PIB e desembolso, com o objetivo de eliminar viés de escalas

> **Lags para análise (lag1 - 1 ano / lag2 - 2 anos):** criada variável dependente (Δlog(PIBpci,t​) e Δlog(PIB_industriapci,t​)), bem como variáveis log(PIB/BNDES​) e log(PIB_industria/BNDES_industria​) com lags.

### 📎 **painel1.parquet** (Delta PIB per capita vs BNDES - Município-Ano)
- **Linhas:** 122.466
- **Merge:** base_pib_hab (LEFT) + base_bndes_total
- **Desembolsos > 0:** 60.959 linhas (2021)

### 📎 **painel2.parquet** (Delta PIB per capita vs BNDES - Estado-Ano)
- **Linhas:** 540 (27 UFs × 20 anos)
- **Agregação:** SUM por Estado-Ano
- **Cálculo:** PIB_per_capita_real recalculado após agregação

### 📎 **painel3.parquet** (Delta Valor Adicionado Indústria vs BNDES - Município-Ano, Indústria)
- **Linhas:** 122.466
- **Merge:** base_pib_hab (LEFT) + base_bndes_industria
- **VA industrial:** va_industria_real, va_industria_per_capita_real

### 📎 **painel4.parquet** (Delta Valor Adicionado Indústria vs BNDES - Estado-Ano, Indústria)
- **Linhas:** 540
- **Agregação:** SUM por Estado-Ano
- **VA industrial per capita:** Recalculado após agregação

---

## 📌 Validações Realizadas

✅ **PIB:**
- Total base_pib_hab (2021): = Total painel [1 e 2] (2021)
- Diferença: 0,00 mil reais

✅ **PIB-Indústria (valor adicionado):**
- Total base_pib_hab (2021): = Total painel [3 e 4] (2021)
- Diferença: 0,00 mil reais

✅ **População:**
- Total base_pib_hab (2021): = Total painel [1,2,3 e 4] (2021)
- Diferença: 0 habitantes

✅ **Desembolsos BNDES:**
- Total base_bndes_total (2021, sem 999999): = Total painel [1 e 2] (2021)
- Diferença: 0,00 mil reais

- Total base_bndes_industria (2021, sem 999999): = Total painel [3 e 4] (2021)
- Diferença: 0,00 mil reais

✅ **Correspondência Município:**
- PIB: 5.570 municípios
- População: 5.570 municípios
- Match 100%

✅ **Balanceamento dos painéis:**
- Nenhum município-Estado perde dados ao longo dos anos dentro da série histórica. Painel mantido como não balanceado.

**Municípios com PIB NaN:**
• AROEIRAS DO ITAIM: 2002, 2003, 2004 (criado em 2005)
• BALNEARIO RINCAO: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 (criado em 2013)
• FIGUEIRAO: 2002, 2003, 2004 (criado em 2005)
• IPIRANGA DO NORTE: 2002, 2003, 2004 (criado em 2005)
• ITANHANGA: 2002, 2003, 2004 (criado em 2005)
• MOJUI DOS CAMPOS: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 (criado em 2013)
• NAZARIA: 2002, 2003, 2004, 2005, 2006, 2007, 2008 (criado em 2011, existiam dados para 2009 e 2010) 
• PARAISO DAS AGUAS: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 (criado em 2013) 
• PESCARIA BRAVA: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 (criado em 2013) 
• PINTO BANDEIRA: 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012 (criado em 2013)

---

## 📌 Notas Importantes

1. **Base 2021 = 100:** Todos os valores financeiros em preços constantes 2021
2. **Unidade:** Mil reais (1.000 reais), inclusive valores per capita
3. **Código município:** Formato IBGE antigo, 6 dígitos (sem verificador)
4. **Período:** 2002–2021 (20 anos completos)
5. **Cobertura:** 100% da população e PIB municipal brasileiro

## 📌 Resumo: Tratamento de Exceções

1️⃣ **CORRESPONDÊNCIA ENTRE MUNICÍPIOS**
- Código de município utilizado na base DATASUS possuía apenas 6 dígitos (sem DV).
- Não havia código de município na base do IBGE.
- Informações de desembolso do BNDES apresentavam código oficial com 7 dígitos (inclui DV).

>**SOLUÇÃO A: Normalização de nomes entre as bases DATASUS e IBGE, com ajustes pontuais para erros de grafia e mudanças de nome histórica.**
    RESULTADO: Convergiu para MATCH 100% - 5.570 municípios únicos.

>**SOLUÇÃO B: Adoção da UF como informação adicional para agrupamento de municípios, evitando agrupar erroneamente homônimos.**
    RESULTADO: Aderência satisfatória, sem união de homônimos.

>**SOLUÇÃO C: Adoção do código de 6 dígitos, descartando DV na base BNDES e utilizando UF como informação adicional para agrupamento.**
    RESULTADO: Aderência satisfatória, com MATCHS esperados.

2️⃣ **COMPLETUDE DOS DADOS**
- Dados de VALOR ADICIONADO INDÚSTRIA ausentes para 2022 e 2023.
- Dados de apoio municipal com código genérico 9999999 na base BNDES. associado a apoio em situações que não se pode precisar a região de destino do recurso.

>⚠️**SOLUÇÃO A: Ajuste no período de análise para 2002 até 2021 (inclusive).**
    RESULTADO: Redução aceitável da cobertura temporal.

>⚠️**SOLUÇÃO B: Descarte das informações de apoio de aproximadamente 743Bi (valores em mil reais 2021)**
    RESULTADO: Redução aceitável da precisão, com fundamentação pertinente e destaque oportuno na conclusão do trabalho.

3️⃣ **ERRO NA BASE DE PIB DO IBGE**
- Havia um valor de PIB negativo para o município de Guamaré (RN) na base do IBGE no ano de 2012.

>**SOLUÇÃO A: Removido valor negativo e convertido em zero**
    RESULTADO: Sem valores ausentes ou NaN em log_PIB_real e log_PIB_per_capita_real nos painéis utilizados.


4️⃣ **DIFERENÇA NOS CONCEITOS DE CLASSIFICAÇÃO DA ATIVIDADE INDUSTRIAL**
- BNDES considera apoio à Eletricidade e gás, água, esgoto, atividades de gestão de resíduos e Construção como apoio à Comércio e Serviços
- IBGE considera apoio à Eletricidade e gás, água, esgoto, atividades de gestão de resíduos e Construção como apoio à indústira

>**SOLUÇÃO A: Identificados os subsetores do CNAE adotados pelo BNDES e redistribuído da classificação Comércio e Serviços para Indústria**
    RESULTADO: Subsetor ELETRICIDADE E GÁS e ÁGUA, ESGOTO E LIXO associados à INDÚSTRIA DE UTILIDADES PÚBLICAS e Subsetor CONSTRUÇÃO associados à INDÚSTRIA DE CONSTRUÇÃO