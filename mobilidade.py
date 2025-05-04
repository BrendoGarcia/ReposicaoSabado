import requests
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
# URLs dos datasets
urls = {
    "transporte_publico": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=9572db54-fbfe-46a2-a31f-ca376085574f&limit=1000",
    "faixas_onibus": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=4e7d896a-8842-4f5d-bf73-01f41b3c1da8&limit=1000",
    "rotas_ciclaveis": "http://dados.recife.pe.gov.br/api/3/action/datastore_search?resource_id=8ae914b6-a087-40cb-b562-3a2ffbcfbd4c&limit=1000",
}

dfs = {}

# Extração
for nome, url in urls.items():
    response = requests.get(url)
    records = response.json()['result']['records']
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
# TRANSFORMAÇÃO DOS DADOS
# ================================

# 🚌 FAIXAS DE ÔNIBUS – tentativa de análise de gargalos
df_faixas = dfs["faixas_onibus"]

# Conversão de coordenadas
if 'latitude' in df_faixas.columns and 'longitude' in df_faixas.columns:
    df_faixas['latitude'] = pd.to_numeric(df_faixas['latitude'], errors='coerce')
    df_faixas['longitude'] = pd.to_numeric(df_faixas['longitude'], errors='coerce')

# Análise de possíveis gargalos por vias
if 'nome' in df_faixas.columns:
    gargalos = df_faixas['nome'].value_counts().reset_index()
    gargalos.columns = ['via', 'ocorrencias']
    print("\n🚧 Gargalos (vias com mais ocorrências de faixas):")
    print(gargalos.head(10))

# 🚲 ROTAS CICLÁVEIS – agrupamento por tipo
df_ciclovias = dfs["rotas_ciclaveis"]
if 'tipo' in df_ciclovias.columns:
    tipos_ciclovia = df_ciclovias['tipo'].value_counts().reset_index()
    tipos_ciclovia.columns = ['tipo', 'quantidade']
    print("\n🚴 Tipos de rotas cicláveis:")
    print(tipos_ciclovia)

# 🚏 TRANSPORTE PÚBLICO – tentativa de padrões de deslocamento
df_transporte = dfs["transporte_publico"]

# Verifica se há colunas de origem/destino
colunas_disponiveis = df_transporte.columns.tolist()
print("\n📊 Colunas disponíveis em transporte público:", colunas_disponiveis)

# Tentativa de análise de horários de pico (se houver alguma coluna relacionada a horário)
colunas_horario = [col for col in df_transporte.columns if 'hora' in col.lower()]
if colunas_horario:
    col_hora = colunas_horario[0]
    df_transporte[col_hora] = pd.to_datetime(df_transporte[col_hora], errors='coerce')
    df_transporte['hora'] = df_transporte[col_hora].dt.hour
    horario_pico = df_transporte['hora'].value_counts().sort_index()

    print("\n🕒 Horários de pico:")
    print(horario_pico)

    # Plot
    horario_pico.plot(kind='bar', title='Horários de Pico - Transporte Público')
    plt.xlabel("Hora do dia")
    plt.ylabel("Quantidade de registros")
    plt.tight_layout()
    plt.show()
else:
    print("\n⚠️ Nenhuma coluna com horário encontrada em transporte público.")

# ================================
# SALVANDO OS DADOS TRANSFORMADOS
# ================================
for nome, df in dfs.items():
    df.to_csv(f"{nome}.csv", index=False)
    

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