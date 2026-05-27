import logging
import os
import json
from datetime import date, timedelta
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import func, update
from sqlalchemy.orm import Session

# Imports do Banco de Dados
from app.db.modelos import BackfillLog
from app.db.conexao import SessionLocal, engine, Base
from app.core import config

# --- IMPORTS CORRIGIDOS (SEM DEPENDER DO main.py) ---
from app.core.helpers import carregar_mapa_pistas, inicializar_banco_seguro
from app.core.driver_factory import configurar_driver_uc, configurar_driver_padrao
from app.core.pipeline_utils import executar_pipeline_site

# --- Scrapers e Processador do BACKFILL ---
from app.scrapers import tf_arquivo_scraper as tf_results_scraper
from app.scrapers import gh_arquivo_scraper as gh_results_scraper
from app.processing.processador_arquivo import processar_e_salvar_dados

def popular_datas_pendentes(db: Session, start_date, end_date):
    logging.info(f"Populando log de backfill de {start_date} até {end_date}...")

    all_required_dates = set()
    current_date = start_date
    while current_date <= end_date:
        all_required_dates.add(current_date)
        current_date += timedelta(days=1)
    
    if not all_required_dates:
        logging.info("Nenhum intervalo de datas para popular.")
        return

    logging.info("Buscando datas já existentes no banco...")
    existing_dates_query = db.query(BackfillLog.data_corrida).filter(
        BackfillLog.data_corrida.between(start_date, end_date)
    )

    existing_dates_in_db = {result[0] for result in existing_dates_query}
    logging.info(f"{len(existing_dates_in_db)} datas encontradas no banco.")
    dates_to_insert = all_required_dates - existing_dates_in_db
    if dates_to_insert:
        logging.info(f"Encontradas {len(dates_to_insert)} novas datas para inserir...")
        datas_para_inserir = [
            BackfillLog(data_corrida=dt, status='pending') for dt in sorted(list(dates_to_insert))
        ]
        
        db.bulk_save_objects(datas_para_inserir)
        db.commit()
        logging.info(f"{len(datas_para_inserir)} novas datas adicionadas ao log.")
    else:
        logging.info("Nenhuma data nova para adicionar. O log já está completo.")

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

def verificar_links_do_dia(data_alvo_str: str):
    logging.info(f"--- Iniciando verificação de integridade para {data_alvo_str} ---")
    
    erros_encontrados = False

    logging.info("Verificando Greyhound (GH)...")
    gh_links_file = os.path.join(config.PASTA_DE_DADOS, f"{data_alvo_str}_links_1_gh.json")
    gh_scraped_file = os.path.join(config.PASTA_DE_DADOS, f"{data_alvo_str}_scraped_1_gh.json")
    
    missing_gh = _comparar_arquivos(gh_links_file, gh_scraped_file, "href_gh")
    
    if missing_gh is None:
        erros_encontrados = True
    elif not missing_gh:
        logging.info("✅ SUCESSO (GH): Todos os links de origem estão no arquivo raspado.")
    else:
        logging.warning(f"🚨 FALHA (GH): {len(missing_gh)} links estão faltando no arquivo raspado:")
        for link in missing_gh:
            logging.warning(f"  - {link}")
        erros_encontrados = True

    logging.info("Verificando Timeform (TF)...")
    tf_links_file = os.path.join(config.PASTA_DE_DADOS, f"{data_alvo_str}_links_1_tf.json")
    tf_scraped_file = os.path.join(config.PASTA_DE_DADOS, f"{data_alvo_str}_scraped_1_tf.json")

    missing_tf = _comparar_arquivos(tf_links_file, tf_scraped_file, "href_tf")
    
    if missing_tf is None:
        erros_encontrados = True
    elif not missing_tf:
        logging.info("✅ SUCESSO (TF): Todos os links de origem estão no arquivo raspado.")
    else:
        logging.warning(f"🚨 FALHA (TF): {len(missing_tf)} links estão faltando no arquivo raspado:")
        for link in missing_tf:
            logging.warning(f"  - {link}")
        erros_encontrados = True

    logging.info(f"--- Verificação Concluída para {data_alvo_str} ---")
    return erros_encontrados

def _comparar_arquivos(links_file_path: str, scraped_file_path: str, link_key: str) -> list | None:

    try:
        with open(links_file_path, 'r', encoding='utf-8') as f:
            links_data = json.load(f)
        source_links = {item[link_key] for item in links_data.get("corridas", []) if link_key in item}
    except FileNotFoundError:
        logging.error(f"Arquivo de links não encontrado: {links_file_path}")
        return None
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logging.error(f"Falha ao ler {links_file_path}. Formato inválido? {e}")
        return None

    if not source_links:
        logging.info(f"Nenhum link de origem encontrado em {links_file_path}. Pulando.")
        return []

    try:
        with open(scraped_file_path, 'r', encoding='utf-8') as f:
            scraped_data = json.load(f)
        scraped_links = {item[link_key] for item in scraped_data if isinstance(item, dict) and link_key in item}
    except FileNotFoundError:
        logging.error(f"Arquivo de dados raspados não encontrado: {scraped_file_path}")
        return sorted(list(source_links)) # Todos os links de origem estão "ausentes"
    except (json.JSONDecodeError, TypeError) as e:
        logging.error(f"Falha ao ler {scraped_file_path}. Formato inválido? {e}")
        return None

    missing_links = source_links - scraped_links

    return sorted(list(missing_links))

def run_results_etl_for_date(target_date: date, mapa_pistas: dict, driver_path: str):
    logging.info(f"### INICIANDO ETL PARA: {target_date.strftime('%Y-%m-%d')} ###")
    
    data_str = target_date.strftime('%Y-%m-%d')
    arquivo_cache_tf = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_results_1_tf.json")
    arquivo_cache_gh = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_results_1_gh.json")

    if os.path.exists(arquivo_cache_tf) and os.path.exists(arquivo_cache_gh):
        logging.info(f"Cache JSON de *Resultados* (TF e GH) encontrado para {data_str}. Pulando Extração.")
    else:
        logging.info(f"Cache JSON de *Resultados* NÃO encontrado para {data_str}. Iniciando Extração.")
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix='Worker') as executor:

            # <--- Submete a Tarefa 1 (Timeform)
            arquivo_tf = executor.submit(
                executar_pipeline_site,
                "Timeform",
                f"{data_str}_links_1_tf.json",
                "1_tf",
                tf_results_scraper.extrair_links_tf,
                tf_results_scraper.raspar_detalhes_pagina_tf,
                mapa_pistas,
                config.URL_BASE_TF,
                lambda: configurar_driver_uc(driver_path),
                (1, 3),
                target_date,
                False
            )
            # <--- Submete a Tarefa 2 (Greyhound)
            arquivo_gh = executor.submit(
                executar_pipeline_site,
                "Greyhound",
                f"{data_str}_links_1_gh.json",
                "1_gh",
                gh_results_scraper.extrair_links_gh,
                gh_results_scraper.raspar_detalhes_pagina_gh,
                mapa_pistas,
                config.URL_BASE_GH,
                lambda: configurar_driver_padrao(driver_path),
                (18, 23),
                target_date,
                True
            )
            arquivo_tf.result()
            arquivo_gh.result()
        logging.info(f"Extração (Scraping) de *Resultados* para {data_str} concluída.")

    logging.info(f"Verificando integridade dos arquivos raspados para {data_str}...")
    
    gh_links_file = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_links_1_gh.json")
    gh_scraped_file = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_1_gh.json")
    missing_gh = _comparar_arquivos(gh_links_file, gh_scraped_file, "href_gh")

    tf_links_file = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_links_1_tf.json")
    tf_scraped_file = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_1_tf.json")
    missing_tf = _comparar_arquivos(tf_links_file, tf_scraped_file, "href_tf")
    
    if (missing_gh and len(missing_gh) > 0) or (missing_tf and len(missing_tf) > 0):
        logging.error(f"Falha na verificação de integridade para {data_str}. Links ausentes:")
        if missing_gh:
            logging.error(f"  GH Faltando: {len(missing_gh)} links")
        if missing_tf:
            logging.error(f"  TF Faltando: {len(missing_tf)} links")
        
        # Isso força o dia a ser marcado como 'failed'
        raise Exception(f"Integridade dos dados falhou para {data_str}. Links ausentes.")
    
    logging.info(f"Integridade verificada com sucesso para {data_str}.")

    logging.info(f"Iniciando Processamento (T & L) de *Resultados* para {data_str}...")
    try:
        processar_e_salvar_dados(target_date) 
        logging.info(f"Processamento de *Resultados* para {data_str} concluído.")
    except Exception as e_process:
        logging.critical(f"[Hist] Erro fatal no Processamento de *Resultados*", exc_info=True)
        raise

def rodar_backfill():
    Base.metadata.create_all(bind=engine, tables=[BackfillLog.__table__], checkfirst=True)
    db = SessionLocal()
    
    DATA_INICIO_PROJETO = date(2021, 1, 1)
    DATA_FIM_PROJETO = date.today() - timedelta(days=1)

    try:
        resetar_trabalhos_presos(db)
        popular_datas_pendentes(db, DATA_INICIO_PROJETO, DATA_FIM_PROJETO)
        mapa_pistas = carregar_mapa_pistas()

        if not mapa_pistas:
            return

        logging.info("Instalando/Verificando o ChromeDriver...")
        driver_path = ChromeDriverManager().install()

        while True:
            trabalho = db.query(BackfillLog).filter(
                BackfillLog.status.in_(['pending', 'failed'])
            ).order_by(BackfillLog.data_corrida.asc()).first()

            if not trabalho:
                logging.info("### BACKFILL CONCLUÍDO! Nenhuma data pendente encontrada. ###")
                break
            
            data_alvo = trabalho.data_corrida
            logging.info(f"=== INICIANDO TRABALHO DE BACKFILL PARA: {data_alvo} ===")

            trabalho.status = 'running'
            trabalho.last_updated = func.now()
            db.commit()

            try:
                run_results_etl_for_date(data_alvo, mapa_pistas, driver_path)
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
            logging.FileHandler("backfill.log", mode='w'),
            #logging.StreamHandler()
        ]
    )
    rodar_backfill()