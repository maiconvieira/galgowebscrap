import json
import os
import logging
from datetime import date, timedelta

# Configuração de logs
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

# Definição do Schema Oficial para Greyhound
SCHEMA_GH = {
    'corrida': [
        'href_gh', # Adicionado para garantir a chave primária
        'pista', 'horario', 'corrida', 'valor', 'categoria', 
        'distancia', 'going_gh', 'forecast', 'tricast', 
        'total_sp_percent', 'nr', 'participantes_resultado'
    ],
    'participante': [
        'posicao_final', 'faixa', 'galgo', 'galgo_url', 
        'cor', 'sex', 'dt_nasc', 'tempo_final_real', 
        'btn_gh', 'sire', 'dam', 'sp_real', 
        'treinador', 'sectional_time', 'remarks_gh'
    ]
}

def normalizar_schema_gh(pasta_dados='data'):
    # Configuração do intervalo de datas para a migração
    data_inicio = date(2021, 1, 1)
    data_fim = date(2021, 5, 20)
    
    logger.info(f"--- Iniciando Normalização de Schema (GH) ---")
    logger.info(f"📅 Período: {data_inicio} até {data_fim}")
    
    data_atual = data_inicio
    arquivos_processados = 0
    corridas_ajustadas = 0

    while data_atual <= data_fim:
        # Define o nome do arquivo alvo
        data_str = data_atual.strftime('%Y-%m-%d')
        nome_arquivo = f"{data_str}_scraped_1_gh.json"
        caminho_arquivo = os.path.join(pasta_dados, nome_arquivo)

        if os.path.exists(caminho_arquivo):
            try:
                with open(caminho_arquivo, 'r', encoding='utf-8') as f:
                    dados = json.load(f)
                
                arquivo_alterado = False
                
                for corrida in dados:
                    # 1. Validação Nível Corrida (Raiz)
                    for campo in SCHEMA_GH['corrida']:
                        if campo not in corrida:
                            corrida[campo] = None
                            arquivo_alterado = True
                            corridas_ajustadas += 1

                    # 2. Validação Nível Participante (Lista)
                    participantes = corrida.get('participantes_resultado')
                    if participantes and isinstance(participantes, list):
                        for p in participantes:
                            for campo_p in SCHEMA_GH['participante']:
                                if campo_p not in p:
                                    p[campo_p] = None
                                    arquivo_alterado = True
                                    # Não contamos ajuste por participante para não poluir o log, 
                                    # mas a flag arquivo_alterado garante o salvamento.

                # Salva apenas se houve alteração (Performance I/O)
                if arquivo_alterado:
                    with open(caminho_arquivo, 'w', encoding='utf-8') as f:
                        json.dump(dados, f, indent=4, ensure_ascii=False)
                    arquivos_processados += 1
                    # logger.info(f"✅ Normalizado: {nome_arquivo}")
                
            except Exception as e:
                logger.error(f"❌ Erro crítico em {nome_arquivo}: {e}")
        
        data_atual += timedelta(days=1)

    logger.info("-" * 50)
    logger.info(f"🚀 PROCESSO CONCLUÍDO!")
    logger.info(f"   - Arquivos modificados: {arquivos_processados}")
    logger.info(f"   - Corridas normalizadas: {corridas_ajustadas} (aprox)")

if __name__ == "__main__":
    # Detecta pasta automaticamente ou usa '.'
    pasta = 'data' if os.path.exists('data') else '.'
    normalizar_schema_gh(pasta_dados=pasta)