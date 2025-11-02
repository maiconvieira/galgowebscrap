import logging
import os
import json
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import Date, Column, String, func, DateTime, update
from sqlalchemy.orm import Session

from app.db.modelos import BackfillLog
from app.db.conexao import SessionLocal, engine, Base
from app.core import config

from main import (
    carregar_mapa_pistas, 
    executar_pipeline_site, 
    configurar_driver_uc, 
    configurar_driver_padrao
)

from app.scrapers import tf_results_scraper, gh_results_scraper
from app.processing.processador_arquivo import processar_e_salvar_dados

def popular_datas_pendentes(db: Session, start_date, end_date):
    logging.info(f"Populando log de backfill de {start_date} até {end_date}...")
    current_date = start_date
    datas_para_inserir = []
    while current_date <= end_date:
        existe = db.query(BackfillLog).filter(BackfillLog.data_corrida == current_date).first()
        if not existe:
            datas_para_inserir.append(BackfillLog(data_corrida=current_date, status='pending'))
        current_date += timedelta(days=1)
    
    if datas_para_inserir:
        db.bulk_save_objects(datas_para_inserir)
        db.commit()
    logging.info(f"{len(datas_para_inserir)} novas datas adicionadas ao log.")

def resetar_trabalhos_presos(db_session: Session):
    try:
        logging.warning("Verificando se há trabalhos travados ('running') de execuções anteriores...")

        stmt = (
            update(BackfillLog)
            .where(BackfillLog.status == 'running')
            .values(status='pending')
        )

        resultado = db_session.execute(stmt)
        num_resetados = resultado.rowcount
        
        if num_resetados > 0:
            db_session.commit()
            logging.warning(f"{num_resetados} trabalhos travados foram resetados para 'pending'.")
        else:
            logging.info("Nenhum trabalho travado encontrado. Tudo limpo.")
            
    except Exception as e:
        logging.error("Erro ao tentar resetar trabalhos presos. Fazendo rollback...", exc_info=True)
        db_session.rollback()
        raise

def run_results_etl_for_date(target_date: date, mapa_pistas: dict):
    logging.info(f"### INICIANDO ETL DE *RESULTADOS* PARA: {target_date.strftime('%Y-%m-%d')} ###")
    
    data_str = target_date.strftime('%Y-%m-%d')
    arquivo_cache_tf = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_results_tf.json")
    arquivo_cache_gh = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_results_gh.json")

    if os.path.exists(arquivo_cache_tf) and os.path.exists(arquivo_cache_gh):
        logging.info(f"Cache JSON de *Resultados* (TF e GH) encontrado. Pulando Extração.")
    else:
        logging.info(f"Cache JSON de *Resultados* NÃO encontrado. Iniciando Extração.")
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix='Worker-H') as executor:

            # <--- MUDANÇA: Submete a Tarefa 1 (Timeform *Resultados*)
            arquivo_tf = executor.submit(
                executar_pipeline_site,
                "Timeform-Results",
                f"{data_str}_links_results_tf.json",
                "results_tf",
                tf_results_scraper.extrair_links_tf,
                tf_results_scraper.raspar_detalhes_pagina_tf_results,
                mapa_pistas,
                config.URL_BASE_TF,
                configurar_driver_uc,
                (1, 3),
                target_date,
                False
            )
            # <--- MUDANÇA: Submete a Tarefa 2 (Greyhound *Resultados*)
            arquivo_gh = executor.submit(
                executar_pipeline_site,
                "Greyhound-Results",
                f"{data_str}_links_results_gh.json",
                "results_gh",
                gh_results_scraper.extrair_links_gh,
                gh_results_scraper.raspar_detalhes_pagina_gh_results,
                mapa_pistas,
                config.URL_BASE_GH,
                configurar_driver_padrao,
                (18, 23),
                target_date,
                True
            )
            arquivo_tf.result()
            arquivo_gh.result()
        logging.info(f"Extração (Scraping) de *Resultados* para {data_str} concluída.")

    logging.info(f"Iniciando Processamento (T & L) de *Resultados* para {data_str}...")
    try:
        # <--- MUDANÇA: Chama o NOVO processador
        processar_e_salvar_resultados(target_date) 
        logging.info(f"Processamento de *Resultados* para {data_str} concluído.")
    except Exception as e_process:
        logging.critical(f"[Hist] Erro fatal no Processamento de *Resultados*", exc_info=True)
        raise

def rodar_backfill():
    Base.metadata.create_all(bind=engine, tables=[BackfillLog.__table__], checkfirst=True)

    db = SessionLocal()
    
    DATA_INICIO_PROJETO = date(2025, 10, 28)
    DATA_FIM_PROJETO = date.today() - timedelta(days=1)

    try:
        resetar_trabalhos_presos(db)
        popular_datas_pendentes(db, DATA_INICIO_PROJETO, DATA_FIM_PROJETO)
        mapa_pistas = carregar_mapa_pistas()

        if not mapa_pistas:
            logging.critical("Mapa de pistas não encontrado. Abortando backfill.")
            return

        while True:
            trabalho = db.query(BackfillLog).filter(
                BackfillLog.status.in_(['pending', 'failed'])
            ).order_by(BackfillLog.data_corrida.desc()).first()

            if not trabalho:
                logging.info("### BACKFILL CONCLUÍDO! Nenhuma data pendente encontrada. ###")
                break
            
            data_alvo = trabalho.data_corrida
            logging.info(f"=== INICIANDO TRABALHO DE BACKFILL PARA: {data_alvo} ===")

            trabalho.status = 'running'
            trabalho.last_updated = func.now()
            db.commit()

            try:
                run_etl_for_date(data_alvo, mapa_pistas)
                trabalho.status = 'success'
                db.commit()
                logging.info(f"=== SUCESSO NO TRABALHO PARA: {data_alvo} ===")

            except Exception as e:
                logging.error(f"!!! FALHA NO TRABALHO PARA: {data_alvo} !!!", exc_info=True)
                db.rollback()
                trabalho.status = 'failed'
                db.commit()
                
    finally:
        db.close()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("backfill.log", mode='w'), # Salva em 'backfill.log'
            #logging.StreamHandler()
        ]
    )
    rodar_backfill()