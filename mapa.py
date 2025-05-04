import folium
from geopy.geocoders import Nominatim
from time import sleep
import pandas as pd
import plotly.express as px
from dash import dcc, html, Input, Output
from pymongo import MongoClient
from jupyter_dash import JupyterDash
import pandas as pd
import matplotlib.pyplot as plt

# Inicializar mapa centralizado em Recife
mapa = folium.Map(location=[-8.0476, -34.8770], zoom_start=12)

# Geocodificador (usamos para obter coordenadas dos bairros)
geolocator = Nominatim(user_agent="recife_criticidade")

# Lista para armazenar erros ou coordenadas ausentes
bairros_nao_encontrados = []

# Loop pelos bairros mais críticos (ex: top 15)
for _, row in df_critico.head(15).iterrows():
    bairro = row['bairro']
    try:
        location = geolocator.geocode(f"{bairro}, Recife, PE, Brasil")
        if location:
            # Adiciona círculo proporcional à criticidade
            folium.CircleMarker(
                location=[location.latitude, location.longitude],
                radius=min(row['indice_critico'] / 10, 30),
                popup=f"{bairro} - Índice: {row['indice_critico']:.1f}",
                color='red',
                fill=True,
                fill_color='red',
                fill_opacity=0.6
            ).add_to(mapa)
        else:
            bairros_nao_encontrados.append(bairro)
    except Exception as e:
        bairros_nao_encontrados.append(bairro)
    sleep(1)  # Respeitar limite do geocoder

# Exibir mapa
mapa.save("mapa_criticidade.html")


# Função para detectar colunas de data e bairro
def detectar_colunas(df):
    col_data = next((col for col in df.columns if 'data' in col.lower()), None)
    col_bairro = next((col for col in df.columns if 'bairro' in col.lower()), None)
    return col_data, col_bairro

# Loop por todos os DataFrames no dicionário dfs
for nome, df in dfs.items():
    print(f"\n📊 Análise do DataFrame: {nome.upper()}")

    col_data, col_bairro = detectar_colunas(df)

    # Tendência Temporal
    if col_data:
        try:
            df[col_data] = pd.to_datetime(df[col_data], errors='coerce')
            df['ano_mes'] = df[col_data].dt.to_period('M')
            tendencia_tempo = df['ano_mes'].value_counts().sort_index()

            print("\n📈 Tendência temporal (quantidade por mês):")
            print(tendencia_tempo)

            tendencia_tempo.plot(kind='line', title=f'Tendência Mensal - {nome}', marker='o')
            plt.xlabel("Ano-Mês")
            plt.ylabel("Quantidade de Registros")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
        except Exception as e:
            print(f"Erro ao processar tendência temporal: {e}")
    else:
        print("❌ Nenhuma coluna de data identificada.")

    # Tendência Geográfica
    if col_bairro:
        try:
            bairros = df[col_bairro].dropna().astype(str)
            tendencia_bairro = bairros.value_counts().head(10)

            print("\n📍 Top 10 Bairros (quantidade de registros):")
            print(tendencia_bairro)

            tendencia_bairro.plot(kind='bar', title=f'Top 10 Bairros - {nome}')
            plt.xlabel("Bairro")
            plt.ylabel("Quantidade")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
        except Exception as e:
            print(f"Erro ao processar tendência geográfica: {e}")
    else:
        print("❌ Nenhuma coluna de bairro identificada.")
        
# Conectar ao MongoDB Atlas
client = MongoClient('mongodb+srv://brendofcg:qwer1234Bb@agrupamentobanco.zb2av.mongodb.net/?retryWrites=true&w=majority&appName=AgrupamentoBanco')
db = client['dados_recife']

# Função para carregar dados do MongoDB
def carregar_dados(colecao):
    try:
        data = list(db[colecao].find({}))
        df = pd.DataFrame(data)
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)
        return df
    except Exception as e:
        print(f"Erro ao carregar dados da coleção {colecao}: {e}")
        return pd.DataFrame()

# Datasets
colecoes = ['unidades_saude', 'leitos', 'atendimentos', 'samu', 'transporte_publico', 'faixas_onibus', 'rotas_ciclaveis']
dataframes = {nome: carregar_dados(nome) for nome in colecoes}

# Debug: mostra estrutura
for nome, df in dataframes.items():
    print(f"{nome}: {df.shape[0]} registros - colunas: {list(df.columns)}")

# Criar app
app = JupyterDash(__name__)

app.layout = html.Div([
    html.H1("Dashboard Interativo de Dados de Recife"),
    dcc.Dropdown(
        id='dataset-dropdown',
        options=[{'label': nome.replace('_', ' ').title(), 'value': nome} for nome in dataframes.keys()],
        value='unidades_saude'
    ),
    dcc.Graph(id='grafico')
])

@app.callback(
    Output('grafico', 'figure'),
    Input('dataset-dropdown', 'value')
)
def atualizar_grafico(dataset):
    df = dataframes.get(dataset)
    if df is None or df.empty:
        return px.histogram(title=f"Nenhum dado encontrado para '{dataset}'")

    if dataset == 'unidades_saude':
        fig = px.histogram(df, x='bairro', title='Unidades de Saúde por Bairro')

    elif dataset == 'leitos':
        fig = px.histogram(df, x='tipo_unidade', title='Leitos por Tipo de Unidade')

    elif dataset == 'atendimentos':
        fig = px.histogram(df, x='bairro', title='Unidade de Pronto Atendimento')

    elif dataset == 'samu':
        if {'latitude', 'longitude'}.issubset(df.columns):
            fig = px.scatter_mapbox(
                df,
                lat='latitude',
                lon='longitude',
                hover_name='nome_oficial',
                zoom=12,
                title='Chamados SAMU',
                height=600
            )
            fig.update_layout(mapbox_style='open-street-map')
        else:
            fig = px.histogram(df, title='Dados insuficientes para mapa SAMU')

    elif dataset == 'transporte_publico':
        if {'latitude', 'longitude'}.issubset(df.columns):
            fig = px.scatter_mapbox(
                df,
                lat='latitude',
                lon='longitude',
                hover_name='nome',
                color='bairro',
                zoom=12,
                title='Pontos de Transporte Público',
                height=600
            )
            fig.update_layout(mapbox_style='open-street-map')
        else:
            fig = px.histogram(df, title='Dados de transporte inválidos')

    elif dataset == 'faixas_onibus':
        if 'bairro' in df.columns:
            fig = px.histogram(df, x='bairro', title='Faixas Exclusivas por Bairro')
        else:
            fig = px.histogram(df, title='Coluna "bairro" não encontrada')

    elif dataset == 'rotas_ciclaveis':
        if {'latitude', 'longitude'}.issubset(df.columns):
            fig = px.scatter_mapbox(
                df,
                lat='latitude',
                lon='longitude',
                hover_name='ruas_envolvidas',
                color='tipo',
                zoom=12,
                title='Rotas Cicláveis em Recife',
                height=600
            )
            fig.update_layout(mapbox_style='open-street-map')
        else:
            fig = px.histogram(df, x='tipo', title='Rotas Cicláveis por Tipo')

    else:
        fig = px.histogram(df, title='Visualização Genérica')

    return fig

# Rodar no Jupyter
app.run(mode='inline')