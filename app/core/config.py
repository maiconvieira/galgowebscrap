from datetime import time as dt_time

# --- Configurações Gerais ---
PASTA_DE_DADOS = "data"
ARQUIVO_MAPA_PISTAS = f"{PASTA_DE_DADOS}/mapa_pistas.json"

# --- Configurações do Scanner ---
HORARIO_CORTE_BUSCA = dt_time(21, 0)

# URLs Base
URL_BASE_GH = "https://greyhoundbet.racingpost.com/"
URL_BASE_TF = "https://www.timeform.com"

# Lista de Pistas a serem ignoradas no Greyhound Bet
PISTAS_EXCLUIDAS_GH = {
    'Youghal', 'Shelbourne Park', 'Thurles', 'Limerick', 'Cork', 
    'Lifford', 'Tralee', 'Mullingar', 'Waterford', 'Dundalk', 
    'Drumbo Park', 'Kilkenny'
}