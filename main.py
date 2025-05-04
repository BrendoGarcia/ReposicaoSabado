import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
from pymongo import MongoClient

# Conectar ao MongoDB
client = MongoClient('mongodb+srv://brendofcg:qwer1234Bb@agrupamentobanco.zb2av.mongodb.net/?retryWrites=true&w=majority&appName=AgrupamentoBanco')
db = client['dados_recife']

# Função para carregar dados
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

# Lista de coleções
colecoes = ['unidades_saude', 'leitos', 'atendimentos', 'samu', 'transporte_publico', 'faixas_onibus', 'rotas_ciclaveis']
dataframes = {nome: carregar_dados(nome) for nome in colecoes}

# Criar o app
app = Dash(__name__)

app.layout = html.Div([
    html.H1("Dashboard Interativo de Dados de Recife"),
    dcc.Dropdown(
        id='dataset-dropdown',
        options=[{'label': nome.replace('_', ' ').title(), 'value': nome} for nome in dataframes.keys()],
        value='unidades_saude'
    ),
    dcc.Graph(id='grafico'),
])

@app.callback(
    Output('grafico', 'figure'),
    Input('dataset-dropdown', 'value')
)
def atualizar_grafico(dataset):
    df = dataframes.get(dataset)
    
    if df is None or df.empty:
        return px.histogram(title=f"Nenhum dado encontrado para '{dataset}'")
    
    # Resetando o gráfico de mapa completamente
    fig = None

    # Para unidades_saude
    if dataset == 'unidades_saude':
        fig = px.histogram(df, x='bairro', title='Unidades de Saúde por Bairro')

    # Para leitos
    elif dataset == 'leitos':
        fig = px.histogram(df, x='tipo_unidade', title='Leitos por Tipo de Unidade')

    # Para atendimentos
    elif dataset == 'atendimentos':
        fig = px.histogram(df, x='bairro', title='Atendimentos por Bairro')

    # Para SAMU (com mapa)
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

    # Para transporte público (com mapa)
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

    # Para faixas de ônibus (gráfico de barras)
    elif dataset == 'faixas_onibus':
        fig = px.histogram(df, x='bairro', title='Faixas Exclusivas por Bairro')

    # Para rotas cicláveis (com mapa)
    elif dataset == 'rotas_ciclaveis':
        if {'latitude', 'longitude', 'tipo'}.issubset(df.columns):
            fig = px.scatter_mapbox(
                df,
                lat='latitude',
                lon='longitude',
                color='tipo',
                hover_name='ruas_envolvidas',
                zoom=12,
                title='Rotas Cicláveis por Tipo',
                height=600
            )
            fig.update_layout(mapbox_style='open-street-map')
        else:
            fig = px.histogram(df, title='Dados insuficientes para mapa de rotas')

    # Caso contrário, exibe um gráfico genérico
    if fig is None:
        fig = px.histogram(df, title='Visualização Genérica')

    return fig

# Rodar o servidor localmente
if __name__ == '__main__':
    app.run(debug=True)
