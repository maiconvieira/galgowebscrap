from datetime import time as dt_time

# --- Configurações Gerais ---
PASTA_DE_DADOS = "data"
ARQUIVO_MAPA_PISTAS = f"{PASTA_DE_DADOS}/mapa_pistas.json"

# --- Configurações do Scanner ---
HORARIO_CORTE_BUSCA = dt_time(21, 0)

# URLs Base
URL_BASE_GH = "https://greyhoundbet.racingpost.com/"
URL_BASE_TF = "https://www.timeform.com"
URL_VIDEO_GH = "https://rp-videos.sisracing.tv/"

# Define as categorias de corrida que aceitamos
CATEGORIAS_PERMITIDAS = {'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9'}

# Define o range de distância que aceitamos
DISTANCIA_MINIMA = 300
DISTANCIA_MAXIMA = 700