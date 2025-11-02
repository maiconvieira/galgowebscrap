import logging
import json
import os
from datetime import date, datetime, timedelta

# Importa as configurações centralizadas
from app.core import config

def padronizar_data(texto_data):
    if not texto_data:
        return None

    formatos_possiveis = [
        '%d/%m/%Y',  # Ex: 25/12/2023
        '%d%b%y',    # Ex: 25Dec23
        '%Y-%m-%d',  # Ex: 2023-12-25
        '%b%y'       # Ex: Dec23
    ]

    for formato in formatos_possiveis:
        try:
            return datetime.strptime(texto_data, formato).date()
        except ValueError:
            continue

    logging.warning(f"Não foi possível padronizar a data '{texto_data}' com os formatos conhecidos.")
    return None

def obter_data_alvo():
    agora = datetime.now()
    hoje = date.today()
    # Usa a configuração importada!
    if agora.time() >= config.HORARIO_CORTE_BUSCA:
        return hoje + timedelta(days=1)
    else:
        #return hoje + timedelta(days=1)
        return hoje

def analisar_posicao_final(texto_data):
    if not texto_data:
        return None

    texto_original = texto_data.strip()
    texto_limpo = texto_original.rstrip('ndrdsth')
    if texto_limpo.isdigit():
        posicao = int(texto_limpo)
        return posicao
    else:
        return texto_original

def json_serializador_de_data(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

def salvar_dados_em_json(dados_para_salvar: list, nome_fonte: str, sufixo_arquivo: str, data_alvo: date):
    if not dados_para_salvar:
        logging.warning(f"Nenhum dado detalhado do {nome_fonte} foi recebido para salvar.")
        return

    data_alvo_str = data_alvo.strftime('%Y-%m-%d')
    nome_arquivo = f"{data_alvo_str}_scraped_{sufixo_arquivo}.json"
    caminho_saida = os.path.join(config.PASTA_DE_DADOS, nome_arquivo)
    chave_id = f"href_{sufixo_arquivo}"
    dados_existentes = []
    
    if os.path.exists(caminho_saida):
        logging.info(f"Arquivo '{caminho_saida}' já existe. Verificando por atualizações...")
        try:
            with open(caminho_saida, 'r', encoding='utf-8') as f:
                dados_existentes = json.load(f)
            if not isinstance(dados_existentes, list):
                logging.warning(f"  -> O arquivo existente não contém uma lista. Será sobrescrito.")
                dados_existentes = []
        except (json.JSONDecodeError, FileNotFoundError):
            logging.warning(f"  -> Arquivo existente corrompido ou ilegível. Será sobrescrito.")
            dados_existentes = []
    else:
        logging.info(f"Arquivo '{caminho_saida}' não encontrado. Será criado um novo.")

    ids_existentes = {corrida.get(chave_id) for corrida in dados_existentes if corrida.get(chave_id)}
    novas_corridas = [corrida for corrida in dados_para_salvar if corrida.get(chave_id) not in ids_existentes]

    if not novas_corridas:
        logging.info(f"-> Nenhum dado novo do {nome_fonte} para adicionar. O arquivo está atualizado.")
        return

    logging.info(f"-> Encontradas {len(novas_corridas)} novas corridas do {nome_fonte}. Adicionando ao arquivo...")

    dados_finais = dados_existentes + novas_corridas

    try:
        with open(caminho_saida, 'w', encoding='utf-8') as f:
            json.dump(dados_finais, f, indent=4, ensure_ascii=False, default=json_serializador_de_data)
        logging.info(f"-> Dados do {nome_fonte} salvos com sucesso em '{caminho_saida}'!")
    except Exception as e:
        logging.error(f"!! ERRO ao salvar o arquivo JSON do {nome_fonte}.", exc_info=True)

def _converter_fracao_para_float(fracao_str: str) -> float | None:
    if not fracao_str or not isinstance(fracao_str, str):
        return None
    
    try:
        fracao_limpa = fracao_str.strip().rstrip('FJ')
        if '/' not in fracao_limpa:
            return None

        partes = fracao_limpa.split('/')
        if len(partes) != 2:
            return None
            
        numerador = float(partes[0].strip())
        denominador = float(partes[1].strip())
        
        if denominador == 0:
            return None
            
        return numerador / denominador
    except (ValueError, TypeError):
        return None

def _limpar_rating_para_int(texto_bruto: str | None) -> int | None:
    if not texto_bruto:
        return None

    texto_limpo = texto_bruto.strip()
    if texto_limpo == "-" or not texto_limpo:
        return None

    try:
        return int(texto_limpo)
    except ValueError:
        logging.warning(f"Valor de rating inesperado encontrado: '{texto_limpo}'. Ignorando.")
        return None