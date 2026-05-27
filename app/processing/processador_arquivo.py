import logging
import json
import os
from datetime import date
from sqlalchemy.orm import Session, joinedload, contains_eager, subqueryload
from app.db.conexao import SessionLocal
from app.db import modelos
from app.core import config

def _carregar_json_resultados(data_alvo: date, sufixo: str):
    data_str = data_alvo.strftime('%Y-%m-%d')
    nome_arquivo = f"{data_str}_results_{sufixo}.json"
    caminho_arquivo = os.path.join(config.PASTA_DE_DADOS, nome_arquivo)

    if not os.path.exists(caminho_arquivo):
        logging.warning(f"[ProcessadorResult] Arquivo de resultados '{caminho_arquivo}' não encontrado.")
        return []
    
    try:
        with open(caminho_arquivo, 'r') as f:
            dados = json.load(f)
        return dados
    except (json.JSONDecodeError, Exception) as e:
        logging.error(f"[ProcessadorResult] Erro ao carregar '{caminho_arquivo}'.", exc_info=True)
        return []

def _criar_mapa_resultados(dados_tf: list, dados_gh: list):
    mapa_resultados = {}
    
    # Priorizamos o Timeform (TF) por ter dados mais ricos (na teoria)
    # Mas na prática, os scrapers de resultado parecem focar nas mesmas coisas.
    
    # Processa Greyhound (GH)
    for corrida in dados_gh:
        href_gh = corrida.get('href_gh')
        href_tf = corrida.get('href_tf') # Assumindo que o merge já foi feito
        chave_href = href_tf or href_gh # Usa a chave que tiver
        
        if not chave_href:
            continue
            
        if chave_href not in mapa_resultados:
            mapa_resultados[chave_href] = {}
            
        for p in corrida.get('participantes_resultado', []):
            faixa = p.get('faixa')
            if faixa:
                # O 'p' contém {'posicao_final', 'faixa', 'sp_real', 'tempo_final_real'}
                mapa_resultados[chave_href][faixa] = p 

    # Processa Timeform (TF) e sobrescreve se necessário
    for corrida in dados_tf:
        href_tf = corrida.get('href_tf')
        if not href_tf:
            continue
            
        if href_tf not in mapa_resultados:
            mapa_resultados[href_tf] = {}
            
        for p in corrida.get('participantes_resultado', []):
            faixa = p.get('faixa')
            if faixa:
                mapa_resultados[href_tf][faixa] = p
                
    return mapa_resultados

def processar_e_salvar_dados(data_alvo: date):
    logging.info(f"--- INICIANDO PROCESSAMENTO DE RESULTADOS PARA {data_alvo} ---")
    
    # 1. Extração (dos JSONs)
    dados_tf = _carregar_json_resultados(data_alvo, 'tf')
    dados_gh = _carregar_json_resultados(data_alvo, 'gh')
    
    if not dados_tf and not dados_gh:
        logging.error(f"Nenhum dado de resultado (TF ou GH) encontrado para {data_alvo}. Abortando.")
        return

    # 2. Transformação (Criação do Mapa O(1))
    logging.info(f"Criando mapa de resultados a partir de {len(dados_tf)} corridas TF e {len(dados_gh)} corridas GH...")
    mapa_resultados = _criar_mapa_resultados(dados_tf, dados_gh)
    
    if not mapa_resultados:
        logging.error(f"Mapa de resultados está vazio. Não há o que processar.")
        return

    # 3. Carga (Load/Update) no Banco
    db: Session = SessionLocal()
    try:
        logging.info(f"Buscando corridas e participantes do dia {data_alvo} no banco...")
        
        # Estratégia de performance:
        # Busca todas as corridas do dia E seus participantes de uma vez.
        # 'subqueryload' é eficiente para carregar coleções (participantes).
        corridas_do_dia = db.query(modelos.Corrida).options(
            subqueryload(modelos.Corrida.participantes)
        ).filter(
            modelos.Corrida.data_corrida == data_alvo
        ).all()
        
        if not corridas_do_dia:
            logging.warning(f"Nenhuma corrida encontrada no banco para {data_alvo}. O 'main.py' rodou para este dia?")
            return

        logging.info(f"{len(corridas_do_dia)} corridas encontradas. Iniciando atualização (merge)...")
        
        total_participantes_atualizados = 0
        
        # Iteramos pelos objetos SQLAlchemy EM MEMÓRIA
        for corrida_db in corridas_do_dia:
            # Tenta achar no mapa pelo href_tf, depois pelo href_gh
            resultados_da_corrida = mapa_resultados.get(corrida_db.href_tf) or mapa_resultados.get(corrida_db.href_gh)
            
            if not resultados_da_corrida:
                # logging.debug(f"Sem resultados no mapa para a corrida: {corrida_db.href_tf}")
                continue

            for p_db in corrida_db.participantes:
                # Se já tiver posição final, pulamos (evita re-processar)
                if p_db.posicao_final is not None:
                    continue
                    
                resultado_do_participante = resultados_da_corrida.get(p_db.faixa)
                
                if not resultado_do_participante:
                    # logging.debug(f"Sem resultado no mapa para a faixa {p_db.faixa} da corrida {corrida_db.href_tf}")
                    continue
                    
                # --- O UPDATE (EM MEMÓRIA) ---
                posicao = resultado_do_participante.get('posicao_final')
                p_db.posicao_final = posicao
                p_db.sp_real = resultado_do_participante.get('sp_real') # TODO: Converter fração para float
                p_db.tempo_final_real = resultado_do_participante.get('tempo_final_real')
                
                if isinstance(posicao, int) and posicao == 1:
                    p_db.foi_vencedor = True
                elif isinstance(posicao, int):
                    p_db.foi_vencedor = False
                else:
                    p_db.foi_vencedor = None # Ex: 'DN' (Did Not Finish)

                total_participantes_atualizados += 1

        if total_participantes_atualizados > 0:
            logging.info(f"Atualização em memória concluída. {total_participantes_atualizados} participantes serão atualizados.")
            logging.info("Enviando transação (commit) para o banco...")
            db.commit()
            logging.info("Commit concluído com sucesso!")
        else:
            logging.info("Nenhum participante novo para atualizar. O banco já estava em dia.")

    except Exception as e:
        logging.critical(f"Erro fatal durante o processamento de resultados. Fazendo rollback...", exc_info=True)
        db.rollback()
        raise # Levanta o erro para o backfill.py marcar como 'failed'
    finally:
        logging.info("Fechando sessão do banco.")
        db.close()