import requests
import pandas as pd

# Dicionário com nomes amigáveis e URLs
urls = {
    "unidades_saude": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=54232db8-ed15-4f1f-90b0-2b5a20eef4cf&limit=1000",
    "leitos": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=d05f6ffa-304b-4a28-bd03-1ffb26cbf866&limit=1000",
    "atendimentos": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=a2dab4d4-3a7b-4cce-b3a7-dd7f5ef22226&limit=1000",
    "profissionais": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=09528351-d546-48ef-8654-42533bd2c8c3&limit=1000",
    "vacinometro": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=a865a988-4b4f-47e8-8e9e-6eaa9ffe9bbf&limit=1000",
    "samu": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=03c831e7-e767-4eb8-816d-e919162bdff0&limit=1000",
}

dfs = {}

# Extração
for nome, url in urls.items():
    response = requests.get(url)
    records = response.json()["result"]["records"]
    dfs[nome] = pd.DataFrame.from_records(records)

# Conectar ao MongoDB Atlas
client = MongoClient('mongodb+srv://brendofcg:qwer1234Bb@agrupamentobanco.zb2av.mongodb.net/?retryWrites=true&w=majority&appName=AgrupamentoBanco')
db = client['dados_recife']  # Nome do banco de dados

# Salvar os dados no MongoDB
for nome, df in dfs.items():
    collection = db[nome]  # Cria uma coleção com o nome do dataset
    data_dict = df.to_dict("records")  # Converte o DataFrame para lista de dicionários
    collection.insert_many(data_dict)  # Insere os dados na coleção

# Visualização inicial
for nome, df in dfs.items():
    print(f"\n--- {nome.upper()} ---")
    print(df.head())

# ================================
# TRANSFORMAÇÕES
# ================================

# 1. UNIDADES DE SAÚDE – cobertura geográfica
df_unidades = dfs["unidades_saude"]
if "bairro" in df_unidades.columns:
    cobertura_bairro = df_unidades["bairro"].value_counts().reset_index()
    cobertura_bairro.columns = ["bairro", "quantidade_unidades"]
    print("\n🏥 Cobertura por bairro (unidades de saúde):")
    print(cobertura_bairro.head(10))

# 2. LEITOS – capacidade total e por tipo
df_leitos = dfs["leitos"]
col_tipo = next((col for col in df_leitos.columns if "tipo" in col.lower()), None)
if col_tipo:
    leitos_tipo = df_leitos[col_tipo].value_counts().reset_index()
    leitos_tipo.columns = ["tipo_leito", "quantidade"]
    print("\n🛏️ Leitos por tipo:")
    print(leitos_tipo)

# 3. ATENDIMENTOS – demanda por unidade
df_atend = dfs["atendimentos"]
col_unidade = next((col for col in df_atend.columns if "unidade" in col.lower()), None)
if col_unidade:
    atendimentos_unidade = df_atend[col_unidade].value_counts().reset_index()
    atendimentos_unidade.columns = ["unidade", "quantidade_atendimentos"]
    print("\n📈 Demanda por unidade de saúde:")
    print(atendimentos_unidade.head(10))

# 4. PROFISSIONAIS – distribuição por categoria
df_profissionais = dfs["profissionais"]
col_prof = next((col for col in df_profissionais.columns if "categoria" in col.lower()), None)
if col_prof:
    dist_prof = df_profissionais[col_prof].value_counts().reset_index()
    dist_prof.columns = ["categoria_profissional", "quantidade"]
    print("\n🩺 Profissionais por categoria:")
    print(dist_prof.head(10))

# 5. SAMU – análise geográfica (se aplicável)
df_samu = dfs["samu"]
if 'latitude' in df_samu.columns and 'longitude' in df_samu.columns:
    df_samu['latitude'] = pd.to_numeric(df_samu['latitude'], errors='coerce')
    df_samu['longitude'] = pd.to_numeric(df_samu['longitude'], errors='coerce')
    print("\n🚑 Pontos de atendimento do SAMU com coordenadas geográficas:")
    print(df_samu[['latitude', 'longitude']].dropna().head())

# ================================
# SALVANDO CSVs
# ================================
for nome, df in dfs.items():
    df.to_csv(f"{nome}.csv", index=False)
    

# Carregar dados transformados da etapa anterior
df_unidades = pd.read_csv("unidades_saude.csv")
df_leitos = pd.read_csv("leitos.csv")
df_atendimentos = pd.read_csv("atendimentos.csv")
df_samu = pd.read_csv("samu.csv")
df_transporte = pd.read_csv("transporte_publico.csv")
df_faixas = pd.read_csv("faixas_onibus.csv")
df_ciclovias = pd.read_csv("rotas_ciclaveis.csv")

import pandas as pd

# Carregar dados transformados da etapa anterior
df_unidades = pd.read_csv("unidades_saude.csv")
df_leitos = pd.read_csv("leitos.csv")
df_atendimentos = pd.read_csv("atendimentos.csv")
df_samu = pd.read_csv("samu.csv")
df_transporte = pd.read_csv("transporte_publico.csv")
df_faixas = pd.read_csv("faixas_onibus.csv")
df_ciclovias = pd.read_csv("rotas_ciclaveis.csv")

# ================================
# AGRUPAMENTO E NORMALIZAÇÃO POR BAIRRO
# ================================

# 1. Unidades de saúde por bairro
saude_por_bairro = df_unidades['bairro'].value_counts().reset_index()
saude_por_bairro.columns = ['bairro', 'qtd_unidades_saude']

# 2. Atendimentos por bairro
col_bairro_atend = next((col for col in df_atendimentos.columns if 'bairro' in col.lower()), None)
if col_bairro_atend:
    atend_por_bairro = df_atendimentos[col_bairro_atend].value_counts().reset_index()
    atend_por_bairro.columns = ['bairro', 'qtd_atendimentos']
else:
    atend_por_bairro = pd.DataFrame(columns=['bairro', 'qtd_atendimentos'])

# 3. Posições do SAMU por bairro
col_bairro_samu = next((col for col in df_samu.columns if 'bairro' in col.lower()), None)
if col_bairro_samu:
    samu_por_bairro = df_samu[col_bairro_samu].value_counts().reset_index()
    samu_por_bairro.columns = ['bairro', 'qtd_chamados_samu']
else:
    samu_por_bairro = pd.DataFrame(columns=['bairro', 'qtd_chamados_samu'])

# 4. Faixas de ônibus por bairro (assumindo que há coluna de bairro)
col_bairro_faixas = next((col for col in df_faixas.columns if 'bairro' in col.lower()), None)
if col_bairro_faixas:
    faixas_por_bairro = df_faixas[col_bairro_faixas].value_counts().reset_index()
    faixas_por_bairro.columns = ['bairro', 'qtd_faixas_onibus']
else:
    faixas_por_bairro = pd.DataFrame(columns=['bairro', 'qtd_faixas_onibus'])

# ================================
# UNIFICAÇÃO DOS DADOS
# ================================

# Unir tudo por bairro
df_critico = saude_por_bairro \
    .merge(atend_por_bairro, on='bairro', how='outer') \
    .merge(samu_por_bairro, on='bairro', how='outer') \
    .merge(faixas_por_bairro, on='bairro', how='outer')

# Preencher valores faltantes com 0
df_critico.fillna(0, inplace=True)

# ================================
# CÁLCULO DE CRITICIDADE (quanto maior, pior)
# ================================
# Peso arbitrário para cada fator – pode ser calibrado
df_critico['indice_critico'] = (
    df_critico['qtd_atendimentos'] * 1.5 +
    df_critico['qtd_chamados_samu'] * 2.0 -
    df_critico['qtd_unidades_saude'] * 1.0 -
    df_critico['qtd_faixas_onibus'] * 0.5
)

# Ranking
df_critico = df_critico.sort_values(by='indice_critico', ascending=False)

# ================================
# RESULTADO FINAL
#📌 Exemplo de Interpretação
#Um bairro com:
#
#Alta demanda de atendimentos
#
#Muitas ocorrências do SAMU
#
#Poucas unidades de saúde
#
#Baixa presença de faixas de ônibus
#
#...terá um índice crítico mais alto, indicando prioridade para intervenção pública.
# ================================
print("\n📍 Ranking de Bairros mais Críticos (Saúde + Mobilidade):")
print(df_critico[['bairro', 'indice_critico']].head(10))

# Salvar relatório final
df_critico.to_csv("relatorio_bairros_criticos.csv", index=False)