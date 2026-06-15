import logging
import os
import time
import json
from datetime import date, timedelta
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import func, update
from sqlalchemy.orm import Session

# Imports do Banco de Dados para controle de progresso do Backfill
from app.db.modelos import BackfillLog
from app.db.conexao import SessionLocal, engine, Base
from app.core import config

# Imports de Extração
from app.core.helpers import carregar_mapa_pistas
from app.core.driver_factory import configurar_driver_uc, configurar_driver_padrao
from app.core.pipeline_utils import executar_pipeline_site
from app.scrapers import tf_arquivo_scraper as tf_results_scraper
from app.scrapers import gh_arquivo_scraper as gh_results_scraper

def popular_datas_pendentes(db: Session, start_date, end_date):
    logging.info(f"Populando log de backfill de {start_date} até {end_date}...")
    all_required_dates = {start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)}

    existing_dates = {dt[0] for dt in db.query(BackfillLog.data_corrida).filter(BackfillLog.data_corrida.between(start_date, end_date))}
    dates_to_insert = all_required_dates - existing_dates
    
    if dates_to_insert:
        db.bulk_save_objects([BackfillLog(data_corrida=dt, status='pending') for dt in sorted(list(dates_to_insert))])
        db.commit()
        logging.info(f"{len(dates_to_insert)} novas datas adicionadas ao log.")

def resetar_trabalhos_presos(db: Session):
    resultado = db.execute(update(BackfillLog).where(BackfillLog.status == 'running').values(status='pending'))
    if resultado.rowcount > 0:
        db.commit()
        logging.warning(f"{resultado.rowcount} trabalhos travados resetados para 'pending'.")

def run_extraction_for_date(target_date: date, mapa_pistas: dict, driver_path: str):
    data_str = target_date.strftime('%Y-%m-%d')
    logging.info(f"### EXTRAINDO DADOS BRUTOS PARA: {data_str} ###")

    arquivo_gh = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_1_gh.json")
    arquivo_tf = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_1_tf.json")

    if os.path.exists(arquivo_tf) and os.path.exists(arquivo_gh):
        logging.info(f"Arquivos JSON brutos já existem para {data_str}. Pulando raspagem.")
        return

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix='Worker') as executor:
        future_tf = executor.submit(
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
            target_date
        )
        
        future_gh = executor.submit(
            executar_pipeline_site,
            "Greyhound",
            f"{data_str}_links_1_gh.json",
            "1_gh",
            gh_results_scraper.extrair_links_gh,
            gh_results_scraper.raspar_detalhes_pagina_gh,
            mapa_pistas,
            config.URL_BASE_GH,
            lambda: configurar_driver_padrao(driver_path),
            (15, 20),
            target_date
        )

        future_tf.result()
        future_gh.result()
        
    logging.info(f"Extração bruta para {data_str} concluída e salva em disco.")

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
        return sorted(list(source_links))
    except (json.JSONDecodeError, TypeError) as e:
        logging.error(f"Falha ao ler {scraped_file_path}. Formato inválido? {e}")
        return None

    missing_links = source_links - scraped_links
    return sorted(list(missing_links))

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

        logging.info("Garantindo ChromeDriver...")
        driver_path = ChromeDriverManager().install()

        while True:
            trabalho = db.query(BackfillLog).filter(
                BackfillLog.status.in_(['pending', 'failed'])
            ).order_by(BackfillLog.data_corrida.asc()).first()

            if not trabalho:
                logging.info("### BACKFILL (FASE 1: EXTRAÇÃO) CONCLUÍDO! ###")
                break
            
            data_alvo = trabalho.data_corrida
            trabalho.status = 'running'
            trabalho.last_updated = func.now()
            db.commit()

            try:
                run_extraction_for_date(data_alvo, mapa_pistas, driver_path)
                trabalho.status = 'success'
                db.commit()

                verificar_links_do_dia(data_alvo.strftime('%Y-%m-%d'))

            except Exception as e:
                logging.error(f"FALHA na extração para: {data_alvo}", exc_info=True)
                db.rollback()
                trabalho.status = 'failed'
                db.commit()
                time.sleep(10)
                
    finally:
        db.close()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("backfill_extracao.log", mode='a', encoding='utf-8')
        ]
    )
    logging.getLogger("undetected_chromedriver").setLevel(logging.ERROR)
    logging.getLogger("selenium").setLevel(logging.ERROR)
    rodar_backfill()