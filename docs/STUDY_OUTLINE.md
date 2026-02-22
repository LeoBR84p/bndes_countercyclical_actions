# Estudo: Evidências da atuação contracíclica do BNDES no nível municipal (2006--2021)
------------------------------------------------------------------------

# Estrutura econométrica:

1. MÉTODO: FE 2-way - Municípios $\alpha{_i}$ + Ano $\lambda{_i}$ <br>

2. AMOSTRA: 2006-2021, painel com 5.770 municípios <br>

3. RHS: $\ X_{i,t} + X_{i,t-1} + X_{i,t-2} + X_{i,t-3}$ + CONTROLES + $\varepsilon_{i,t} $

4. VARIÁVEL INDEPENDENTE X = $\frac{Desembolso_{i,t}}{PIB_{t-1}}$ <br>

5. CONTROLES: $\ logPIBpc_{i,t} + share\_ind_{(i,t-1)} + share\_agro_{(i,t-1)} + logPop_{i,t-1}$ <br> onde $\ share\_ind $ e $share\_agro $ refletem a participação do valor adicionado no PIB pelos setores da indústria e agropecuária, respectivamente, para cada município em razão ao PIB do ano anterior. 

6. SE (principal): cluster por município <br> $ \operatorname{Cov}(\varepsilon_{i,t}, \varepsilon_{i,s}) \neq 0 $ para qualquer t $\neq$ de s; E<br> $ \operatorname{Cov}(\varepsilon_{i,t}, \varepsilon_{j,s}) = 0 $ para qualquer i $\neq$ de j. <br> Ou seja, é permitido que os fatores de erro do mesmo município estejam correlacionados no tempo, mas assume-se que são independentes para municípios diferentes. <br> 

7. SE (robustez $_1$): cluster two-way município + ano <br>  $ \operatorname{Cov}(\varepsilon_{i,t}, \varepsilon_{i,s}) \neq 0 $ para qualquer t $\neq$ de s; E<br> $ \operatorname{Cov}(\varepsilon_{i,t}, \varepsilon_{j,s}) \neq 0 $ para qualquer i $\neq$ de j; E<br> $ \operatorname{Cov}(\varepsilon_{i,t}, \varepsilon_{j,s}) = 0 $ se i $\neq$ de j e t $\neq$ de s. <br> Ou seja, é permitido que os fatores de erro do mesmo município estejam correlacionados no tempo e também entre municípios, mas neste caso apenas dentro do mesmo ano. <br> 

> Nota: Optou-se por cluster two-way no lugar de Driscoll-Kraay, pois a janela temporal era reduzida (16 anos)

8. SE (robustez $_2$): cluster por UF <br> Se $\ g_{(i)} $ é a UF do município i<br> $ \operatorname{Cov}(\varepsilon_{i,t}, \varepsilon_{j,s}) \neq 0 $ se $\ g_{(i)} $ = $\ g_{(j)} $; E <br> $ \operatorname{Cov}(\varepsilon_{i,t}, \varepsilon_{j,s}) = 0 $ se $\ g_{(i)} \neq \ g_{(j)} $. <br> Ou seja, é permitido que os fatores de erro do município estejam correlacionados entre municípios do mesmo Estado e ao longo do tempo, mas assume-se que são independentes para Estados diferentes. <br> 


+++ SALVAR +++
$$ \Delta y_{it} = \sum_{k=0}^{3} \beta_k X_{i,t-k} + \sum_{h=1}^{2} \theta_h X_{i,t+h} + \sum_{k=0}^{3} \delta_k \left( X_{i,t-k} \times \widetilde{\log POP}_{i,t-1} \right) + \sum_{h=1}^{2} \phi_h \left( X_{i,t+h} \times \widetilde{\log POP}_{i,t-1} \right) + \gamma' Z_{i,t-1} + \alpha_i + \lambda_t + \varepsilon_{it} $$


| MODELO BASELINE (1) <br> $\Delta \log(PIB_{i,t})$ | MODELO COMPARATIVO 1 <br> $\Delta \log(PIBpc_{i,t})$  | TESTE PRETREND 1 <br> 2 Leads no MODELO BASELINE | MODELO COMPARATIVO 2 <br> função de reação  $\ y:X_{i,t} $| ESTUDO DE CASO 1 <br> $\Delta \log(PIB_{i,t})$ | ESTUDO DE CASO 2 <br> $\Delta \log(PIBpc_{i,t})$ | ESTUDO DE CASO 3 <br>  $\Delta \operatorname{asinh}(VA\_ind_{it})$ | ESTUDO DE CASO 4 <br>  $\Delta \operatorname{asinh}(VA\_agro_{it})$ | TESTE DE ROBUSTEZ DO DENOMINADOR <br> X = $Desembolso\_pc_{i,t}$ | TESTE PRETREND 2 <br> 2 Leads no TESTE DO DENOMINADOR | TESTE DE HETEROGENEIDADE <br> aplicando variável contínua |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Equação 1:**<br>$\Delta \log(PIB_{it}) = \beta X_{it} + \sum_{k=1}^{3} \beta_k X_{i,t-k} + {\color{blue}\mathbf{\theta_1 X_{i,t+1} + \theta_2 X_{i,t+2}}}+ \gamma_1 \log(PIBpc_{i,t-1}) + \gamma_2 shareind_{i,t-1} + \gamma_3 shareagro_{i,t-1} + \gamma_4 \log(pop_{i,t-1}) +  \alpha_i + \gamma_t + \varepsilon_{it}$<br>| **Equação 2:**<br> ${\color{red}\mathbf{\Delta \operatorname{asinh}(VA\_ind_{it})}} = \beta X_{it} + \sum_{k=1}^{3} \beta_k X_{i,t-k} + {\color{blue}\mathbf{\theta_1 X_{i,t+1} + \theta_2 X_{i,t+2}}} + \gamma_1 log(PIBpc_{i,t-1}) + \gamma_2 shareind_{i,t-1} + \gamma_3 shareagro_{i,t-1} + \gamma_4 \log(pop_{i,t-1}) + \alpha_i + \gamma_t + \varepsilon_{it}$<br>| **Equação 3:**<br> ${\color{red}\mathbf{\Delta \operatorname{asinh}(VA\_agro_{it})}} = \beta X_{it} + \sum_{k=1}^{3} \beta_k X_{i,t-k} + {\color{blue}\mathbf{\theta_1 X_{i,t+1} + \theta_2 X_{i,t+2}}} + \gamma_1 log(PIBpc_{i,t-1}) + \gamma_2 shareind_{i,t-1} + \gamma_3 shareagro_{i,t-1} + \gamma_4 \log(pop_{i,t-1}) + \alpha_i + \gamma_t + \varepsilon_{it}$<br>|**Equação 4:**<br> ${\color{red}\mathbf{\Delta \log(PIBpc_{it})}} = \beta X_{it} + \sum_{k=1}^{3} \beta_k X_{i,t-k} + {\color{blue}\mathbf{\theta_1 X_{i,t+1} + \theta_2 X_{i,t+2}}} + \gamma_1 \log(PIBpc_{i,t-1}) + \gamma_2 shareind_{i,t-1} + \gamma_3 shareagro_{i,t-1} + \alpha_i + \gamma_t + \varepsilon_{it}$<br>|
| **Onde:**<br>- $\Delta \log(PIB_{it})$: Crescimento real do PIB do município 𝑖 no ano 𝑡.<br>É aproximadamente a taxa percentual de crescimento. Usar diferença em log reduz tendência estrutural e facilita interpretação como elasticidade.<br><br>- $\beta X_{it}$ = $\beta\frac{Desembolso_{i,t}}{PIB_{i,t}}$.<br>Efeito contemporâneo do desembolso (share) sobre o crescimento no mesmo ano.<br> Se significativo, indica impacto de curto prazo do crédito.<br><br>- $\sum_{k=1}^{3} \beta_k X_{i,t-k}$: Efeitos defasados do desembolso de até 3 anos atrás. Captura o impacto distribuído no tempo, importante porque investimento financiado pode gerar efeito gradual.<br><br>- $\beta_1$ → efeito após 1 ano ... $\beta_3$ → efeito até 3 anos depois.<br>A soma dos $\beta_k$ indica efeito acumulado de médio prazo.<br><br>- $\theta_1 X_{i,t+1}$: Lead placebo de 1 ano, definida como $\frac{Desembolso_{i,t+1}}{PIB_{i,t}}$.<br>Testa se crescimento atual estaria associado a desembolso futuro. Se significativo → possível pré-tendência ou endogeneidade.<br><br>- $\theta_2 X_{i,t+2}$: Lead placebo de 2 anos, definida como $\frac{Desembolso_{i,t+2}}{PIB_{i,t+1}}$.<br>Teste adicional de falsificação para padrões antecipatórios mais longos.<br><br>- $\gamma_1 \log(PIBpc_{i,t-1})$: Controle predeterminado do nível de renda per capita no ano anterior.<br>Captura convergência econômica: Municípios mais ricos tendem a crescer menos (efeito de convergência). Evita confundir crescimento com estágio de desenvolvimento inicial.<br><br>- $\gamma_2 shareind_{i,t-1}$: Participação da indústria no PIB no ano anterior.<br>Controla a estrutura produtiva local. Municípios mais industriais podem ter crescimento mais volátil. Reduz viés estrutural entre economias industriais e primárias.<br><br>- $\gamma_3 shareagro_{i,t-1}$: Participação do setor agreopecuário no PIB no ano anterior.<br>Controla a estrutura produtiva local. Reduz viés estrutural entre economias industriais e primárias.<br><br>- $\gamma_4 \log(pop_{i,t-1})$: Tamanho populacional no ano anterior.<br>Controla escala econômica e dinâmica demográfica. Municípios maiores podem ter crescimento diferente por economia de escala. Evita confundir efeito do crédito com tamanho estrutural.<br><br>- $\alpha_i$: Efeito fixo do município.<br>Controla características invariantes no tempo: geografia, cultura produtiva, questões estruturais e localização.Remove heterogeneidade não observada fixa.<br><br>- $\gamma_t$: Efeito fixo de ano.<br>Controla choques macroeconômicos comuns: ciclo econômico nacional, crises, política monetária e choques externos. Isola variação relativa entre municípios.<br><br>- $\varepsilon_{it}$: Erro idiossincrático.<br>Componente não explicado pelo modelo. Deve ser tratado com erro padrão robusto/clusterizado. | **Onde:**<br>- $\Delta \operatorname{asinh}(VA\_ind_{it})$: Crescimento real do valor adicionado ao PIB pelo setor industrial do município 𝑖 no ano 𝑡.<br><br>- $\beta X_{it}$ = $\beta\frac{Desembolso\_indústria_{i,t}}{PIB_{i,t}}$.<br><br>- $\sum_{k=1}^{3} \beta_k X_{i,t-k}$: Efeitos defasados do desembolso de até 3 anos atrás. <br><br>- $\beta_1$ → efeito após 1 ano ... $\beta_3$ → efeito até 3 anos depois.<br><br>- $\theta_1 X_{i,t+1}$: Lead placebo de 1 ano, definida como $\frac{Desembolso\_indústria_{i,t+1}}{PIB_{i,t}}$.<br>Testa se crescimento atual estaria associado a desembolso futuro. Se significativo → possível pré-tendência ou endogeneidade.<br><br>- $\theta_2 X_{i,t+2}$: Lead placebo de 2 anos, definida como $\frac{Desembolso\_indústria_{i,t+2}}{PIB_{i,t+1}}$.<br>Teste adicional de falsificação para padrões antecipatórios mais longos.<br><br>- $\gamma_1 \log(PIBpc_{i,t-1})$: Controle predeterminado do nível de renda per capita no ano anterior.<br><br>- $\gamma_2 shareind_{i,t-1}$: Participação da indústria no PIB no ano anterior.<br><br>- $\gamma_3 shareagro_{i,t-1}$: Participação do setor agreopecuário no PIB no ano anterior.<br><br>- $\gamma_4 \log(pop_{i,t-1})$: Tamanho populacional no ano anterior.<br><br>- $\alpha_i$: Efeito fixo do município.<br><br>- $\gamma_t$: Efeito fixo de ano.<br><br>- $\varepsilon_{it}$: Erro idiossincrático. | **Onde:**<br>- $\Delta \operatorname{asinh}(VA\_agro_{it})$: Crescimento real do valor adicionado ao PIB pelo setor agropecuário do município 𝑖 no ano 𝑡.<br><br>- $\beta X_{it}$ = $\beta\frac{Desembolso\_agropecuária_{i,t}}{PIB_{i,t}}$.<br><br>- $\sum_{k=1}^{3} \beta_k X_{i,t-k}$: Efeitos defasados do desembolso de até 3 anos atrás. <br><br>- $\beta_1$ → efeito após 1 ano ... $\beta_3$ → efeito até 3 anos depois.<br><br>- $\theta_1 X_{i,t+1}$: Lead placebo de 1 ano, definida como $\frac{Desembolso\_agropecuária_{i,t+1}}{PIB_{i,t}}$.<br>Testa se crescimento atual estaria associado a desembolso futuro. Se significativo → possível pré-tendência ou endogeneidade.<br><br>- $\theta_2 X_{i,t+2}$: Lead placebo de 2 anos, definida como $\frac{Desembolso\_agropecuária_{i,t+2}}{PIB_{i,t+1}}$.<br>Teste adicional de falsificação para padrões antecipatórios mais longos.<br><br>- $\gamma_1 \log(PIBpc_{i,t-1})$: Controle predeterminado do nível de renda per capita no ano anterior.<br><br>- $\gamma_2 shareind_{i,t-1}$: Participação da indústria no PIB no ano anterior.<br><br>- $\gamma_3 shareagro_{i,t-1}$: Participação do setor agreopecuário no PIB no ano anterior.<br><br>- $\gamma_4 \log(pop_{i,t-1})$: Tamanho populacional no ano anterior.<br><br>- $\alpha_i$: Efeito fixo do município.<br><br>- $\gamma_t$: Efeito fixo de ano.<br><br>- $\varepsilon_{it}$: Erro idiossincrático. | **Onde:**<br>- $\Delta log(PIBpc_{it})$: Crescimento real do PIB per capita do município 𝑖 no ano 𝑡.<br><br>- $\beta X_{it}$ = $\beta\frac{Desembolso\_indústria_{i,t}}{PIB_{i,t}}$.<br>Efeito contemporâneo do desembolso para o setor industrial (share) sobre o crescimento do PIB total no mesmo ano.<br><br>- $\sum_{k=1}^{3} \beta_k X_{i,t-k}$: Efeitos defasados do desembolso de até 3 anos atrás. <br><br>- $\beta_1$ → efeito após 1 ano ... $\beta_3$ → efeito até 3 anos depois.<br><br>- $\theta_1 X_{i,t+1}$: Lead placebo de 1 ano, definida como $\frac{Desembolso\_indústria_{i,t+1}}{PIB_{i,t}}$.<br>Testa se crescimento atual estaria associado a desembolso futuro. Se significativo → possível pré-tendência ou endogeneidade.<br><br>- $\theta_2 X_{i,t+2}$: Lead placebo de 2 anos, definida como $\frac{Desembolso\_indústria_{i,t+2}}{PIB_{i,t+1}}$.<br>Teste adicional de falsificação para padrões antecipatórios mais longos.<br><br>- $\gamma_1 \log(PIBpc_{i,t-1})$: Controle predeterminado do nível de renda per capita no ano anterior.<br><br>- $\gamma_2 shareind_{i,t-1}$: Participação da indústria no PIB no ano anterior.<br><br>- $\gamma_3 shareagro_{i,t-1}$: Participação do setor agreopecuário no PIB no ano anterior.<br><br>- $\alpha_i$: Efeito fixo do município.<br><br>- $\gamma_t$: Efeito fixo de ano.<br><br>- $\varepsilon_{it}$: Erro idiossincrático. | 

<br>

# 🎯 1. Pergunta de Pesquisa e Unidade de Análise

1.  **Definição da pergunta principal**\
    Crescimento econômico agregado Δlog (PIB real).\
    → Implica foco em expansão da economia local, não apenas renda média.

2.  **Unidade principal: Município**\
    → Maximiza variação e poder estatístico.\
    → Permite captar heterogeneidade local.

3.  **Desfecho complementar**\
    Crescimento econômico agregado Δasih (VA_ind) e Δasih (VA_agro).\
    → Permite verificar se o crescimento do setor industrial ou agropecuário é impactado.

4.  **Estudo comparativo: para modelos relevantes**\
    Δlog PIB real per capita.\
    → Permite verificar se o crescimento não é apenas demográfico, reforçando robustez interpretativa.

<br>

------------------------------------------------------------------------

# 📊 2. Construção das Variáveis Principais

**Objetivo:** Permitir inferência clara do tipo "A% do PIB em desembolsos → B% de crescimento".

1.  **Y principal: Δlog(PIB real)**\
    → Interpretação em termos percentuais de crescimento.

2.  **Y alternativas: Δasih (VA_ind) e Δasih (VA_agro)**\
    → Interpretação em termos percentuais de crescimento para setores específicos: indústria e agropecuária.

3.  **X principal: Share em nível**\
    $x_{it} = \frac{Desemb_{it}}{PIB_{i,t}}$ → Mede intensidade do apoio.\
    → Evita problema mecânico do denominador contemporâneo.\
    → Permite interpretação direta em pontos percentuais do PIB.

4.  **Sem log no share**\
    → Evita problema com zeros.\
    → Mantém clareza interpretativa para política pública.

5.  **Deflação consistente**\
    → PIB real (IBGE).\
    → Desembolsos deflacionados pelo deflator implícito do PIB para garantir comparabilidade intertemporal.

📌 **Destaque no paper:**\
Foi utilizado PIB defasado no denominador (para evitar endogeneidade mecânica).

------------------------------------------------------------------------

# ⏳ 3. Dinâmica Temporal

**Objetivo:** Capturar timing econômico plausível e evitar atribuir efeitos contemporâneos irreais a investimentos com maturação lenta.

1.  **Fluxo anual com lags 1--3**\
    → Permite captar efeitos graduais.

2.  **Headline: efeito acumulado 1--3 anos**\
    → Representa impacto de curto/médio prazo.

3.  **Complementar: acumulado 1--5 anos**\
    → Captura maturação de projetos estruturantes.

4.  **Leads placebo ($x_{t+1}, x_{t+2}$)**\
    → Teste de causalidade reversa.\
    → Se significativos, indicam possível seleção dinâmica.

📌 **Destaque no paper:**\
Gráfico dos coeficientes por lag com IC 95%.

<span style="color:red">**ATENÇÃO: Lead placedo devem apresentar Beta próximo de ZERO (ou seja, desembolsos futuros não devem explicar crescimento corrente).**</span>

------------------------------------------------------------------------

# 🏗 4. Estrutura do Modelo e Identificação

1.  **FE Município + Ano**\
    → Controla heterogeneidade fixa e choques nacionais.

2.  **Controles predeterminados (t-1)**\
    → log(PIBpc real)\
    → share indústria\
    → share agropecuária\
    → log(população)\
    → Capturam nível de desenvolvimento e estrutura produtiva.

3.  **UF×trend como robustez**\
    → Permite tendências regionais específicas.

4.  **Sem IV formal**\
    → Indisponibilidade prática de variável associada a desembolso, mas exógena ao PIB.\
    → Substituído por estudo associativo robusto.\
    → Causalidade defendida por análise de timing e falsificação temporal.

📌 **Destaque no paper:**\
Explicar claramente a diferença entre associação robusta e causalidade estrutural. 

<span style="color:red">**ATENÇÃO: Ausência de IV formal se torna uma limitação, mas que pode ser tolerada. Não argumentar “prova causal”, mas verificar a possibilidade de crescimento “associado a”, “consistente com”, “com evidências que sugerem”. Ao quantificar “quanto do crescimento”: chamar de decomposição modelada (condicional ao modelo).</span>

------------------------------------------------------------------------

# 🧪 5. Robustez e Falsificação

**Objetivo:** Demonstrar que o achado não depende de especificação particular.

1.  **X alternativo: desembolsos per capita**\
    → Verifica robustez à escala populacional.

2.  **Subamostras**\
    → Quartis de share_ind\
    → Quartis de população\
    → Testa heterogeneidade estrutural.

3.  **Exclusão de extremos**\
    → Top 1% de X\
    → Top e bottom 1% de Y\
    → Evita dominância por outliers.

4.  **Dependência espacial (Conley)**\
    → Corrige possível correlação espacial residual.

📌 **Destaque no paper:**\
Tabela consolidando estabilidade do coeficiente acumulado 1--3 anos.

------------------------------------------------------------------------

# 📐 6. Inferência Estatística

**Objetivo:** Garantir validade dos testes estatísticos e evitar significância artificial.

1.  **Erros-padrão principais: two-way cluster (município + ano)**\
    → Permite correlação serial e choques comuns.

2.  **Robustez: cluster município + UF**\
    → Captura dependência regional adicional.

3.  **Painel estadual com cautela**\
    → N pequeno (27 UFs) exige interpretação conservadora.

📌 **Destaque no paper:**\
Explicar por que cluster simples seria inadequado.

------------------------------------------------------------------------

# 📈 7. Decomposição do Crescimento

**Objetivo:** Traduz β em contribuição econômica concreta, estimando "quanto do crescimento no período é associado ao apoio".

1.  **Cálculo da contribuição modelada**\
    $\hat{contrib}_{it} = \sum_k \hat{\beta}_k \, x_{i,t-k}$

2.  **Agregação no período 2002--2021**\
    → Comparação entre crescimento observado e crescimento previsto pelo canal BNDES.

3.  **Linguagem adequada**\
    → "Contribuição estimada condicional ao modelo".

📌 **Destaque no paper:**\
Deixar explícito que se trata de decomposição modelada, não prova causal definitiva.

------------------------------------------------------------------------

# ⚠️ 8. Limitações

**Objetivo:** Delimitar escopo interpretativo.

1.  Endogeneidade residual possível.\
    → O BNDES não aloca recursos aleatoriamente.\
    → Mesmo com:FE município + ano, Controles estruturais (PIBpc, indústria, população), Lags 1–5, Placebos com leads, ainda pode existir seleção dinâmica não observada.\
    → Desembolsos podem estar correlacionados com:
> -Expectativas de crescimento futuro,\
> -Projetos já em maturação,\
> -Estratégias regionais específicas,\
> -Pressões políticas ou setoriais.

2.  Mensuração municipal incompleta.\
    → Parte dos desembolsos não possui georreferenciamento municipal, o que ocasiona subestimação do efeito municipal (attenuation bias - enviesado para baixo).\
    [**Efeito mitigado com modelo estadual, comparação entre padrão municipal e estadual e argumentação de spillover entre municípios**]\
    → Desembolso pode ser registrada no local do tomador e não do investimento real.

3.  Ausência de IV formal.\
    → Ausência de variável associada à desembolso e exógena ao PIB classifica o estudo como associativo.\
    → A interpretação casual depende de suposições implícitas.\
    [**Credibilidade fortalecida por timing coerente, falsificação temporal, controles estruturais e robustes**]

4.  Choques de políticas regionais e setoriais concorrentes não modelados explicitamente.\
    → Entre 2002 e 2021 ocorreram: boom e queda de commodities, crises fiscais estaduais, políticas federais paralelas e mudanças institucionais.\
    → Mesmo com FE de ano, podem existir: políticas regionais específicas, choques setoriais diferenciados e mudanças regulatórias locais.

📌 **Destaque no paper:**\
Seção própria de limitações antes da conclusão.

1. Endogeneidade residual: “Embora o desenho controle por heterogeneidade fixa e características estruturais predeterminadas, não é possível descartar integralmente endogeneidade residual decorrente de seleção dinâmica.”

2. Mensuração municipal incompleta: “A mensuração municipal dos desembolsos pode não refletir integralmente o local de incidência econômica do investimento.”

3. Ausência de IV formal: “A indisponibilidade de um instrumento exógeno impede interpretação causal estrita, embora a consistência temporal e estrutural dos resultados seja compatível com efeito econômico.”

4. Choques de políticas regionais e setoriais concorrentes não modelados explicitamente: “Embora efeitos fixos e tendências regionais reduzam viés por choques agregados, políticas regionais e setoriais concorrentes não são explicitamente modeladas.”

------------------------------------------------------------------------

# 📑 Estrutura de Apresentação

1.  Tabela principal (acumulado 1--3).\
2.  Tabela complementar (1--5).\
3.  Tabela de robustez consolidada.\
4.  Figura dinâmica dos lags.\
5.  Tabela estadual comparativa.
