import os
os.environ['WDM_LOG'] = '0'
os.environ['WDM_PRINT_FIRST_LINE'] = 'False'

import time
import json
import logging
from datetime import date, timedelta
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import func, update
from sqlalchemy.orm import Session
from app.db.modelos import BackfillLog
from app.db.conexao import SessionLocal, engine, Base
from app.core import config
from app.core.helpers import carregar_mapa_pistas
from app.core.driver_factory import configurar_driver_uc, configurar_driver_padrao
from app.core.pipeline_utils import executar_pipeline_site
from app.scrapers import tf_arquivo_scraper as tf_results_scraper
from app.scrapers import gh_arquivo_scraper as gh_results_scraper

class TaxonomiaBackfillFormatter(logging.Formatter):
    def format(self, record):
        data_exec = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        
        t_name = record.threadName
        if t_name == "MainThread":
            thread_id = "SYS"
        elif t_name.startswith("Worker_"):
            thread_id = f"W{t_name.split('_')[1]}"
        else:
            thread_id = t_name[:3].upper()

        status = record.levelname
        if status == "WARNING": status = "WARN"
        elif status == "ERROR": status = "ERR "
        elif status == "CRITICAL": status = "CRIT"
        elif status == "DEBUG": status = "DEBG"
        else: status = status[:4].upper()
        
        return f"[{data_exec}][{thread_id:<3}][{record.name.split('.')[-1][:4].upper()}][{status:<4}] {record.getMessage()}"

def popular_datas_pendentes(db: Session, start_date, end_date):
    logging.info(f"Populando log de backfill de {start_date} até {end_date}")
    all_required_dates = {start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)}

    existing_dates = {dt[0] for dt in db.query(BackfillLog.data_corrida).filter(BackfillLog.data_corrida.between(start_date, end_date))}
    dates_to_insert = all_required_dates - existing_dates
    
    if dates_to_insert:
        db.bulk_save_objects([BackfillLog(data_corrida=dt, status='pending') for dt in sorted(list(dates_to_insert))])
        db.commit()
        logging.info(f"Adicionadas {len(dates_to_insert)} novas datas pendentes ao banco.")

def resetar_trabalhos_presos(db: Session):
    resultado = db.execute(update(BackfillLog).where(BackfillLog.status == 'running').values(status='pending'))
    if resultado.rowcount > 0:
        db.commit()
        logging.warning(f"Reset de emergência: {resultado.rowcount} trabalhos reativados para 'pending'.")

def run_extraction_for_date(target_date: date, mapa_pistas: dict, driver_path: str):
    data_str = target_date.strftime('%Y-%m-%d')
    logging.info(f"Iniciando extração bruta para a data alvo: {data_str}")

    arquivo_gh = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_1_gh.json")
    arquivo_tf = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_1_tf.json")

    if os.path.exists(arquivo_tf) and os.path.exists(arquivo_gh):
        logging.info(f"Arquivos JSON já existem para {data_str}. Ponto de verificação atingido.")
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
        
    logging.info(f"Processamento concluído e salvo em disco para a data {data_str}.")

def verificar_links_do_dia(data_alvo_str: str):
    logging.info(f"Iniciando verificação de integridade bidirecional para {data_alvo_str}.")
    erros_encontrados = False

    configuracoes = [
        ("Greyhound", "gh", "href_gh"),
        ("Timeform", "tf", "href_tf")
    ]

    for nome_casa, sufixo, chave_link in configuracoes:
        links_file = os.path.join(config.PASTA_DE_DADOS, f"{data_alvo_str}_links_1_{sufixo}.json")
        scraped_file = os.path.join(config.PASTA_DE_DADOS, f"{data_alvo_str}_scraped_1_{sufixo}.json")
        
        missing_links = _comparar_arquivos(links_file, scraped_file, chave_link)
        
        if missing_links is None:
            erros_encontrados = True
        elif not missing_links:
            logging.info(f"Paridade validada com sucesso no {nome_casa} (100%).")
        else:
            logging.warning(f"Falha de integridade {sufixo.upper()}: Omissão de {len(missing_links)} links no arquivo extraído.")
            erros_encontrados = True

    return erros_encontrados

def _comparar_arquivos(links_file_path: str, scraped_file_path: str, link_key: str) -> list | None:
    try:
        with open(links_file_path, 'r', encoding='utf-8') as f:
            links_data = json.load(f)
        source_links = {item[link_key] for item in links_data.get("corridas", []) if link_key in item}
    except FileNotFoundError:
        logging.error(f"Arquivo origem inexistente: {links_file_path}")
        return None
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logging.error(f"Formatação inválida no arquivo origem {links_file_path}: {e}")
        return None

    if not source_links:
        logging.info(f"Conjunto de links vazio no arquivo {links_file_path}. Operação ignorada.")
        return []

    try:
        with open(scraped_file_path, 'r', encoding='utf-8') as f:
            scraped_data = json.load(f)
        scraped_links = {item[link_key] for item in scraped_data if isinstance(item, dict) and link_key in item}
    except FileNotFoundError:
        logging.error(f"Arquivo alvo inexistente: {scraped_file_path}")
        return sorted(list(source_links))
    except (json.JSONDecodeError, TypeError) as e:
        logging.error(f"Formatação inválida no arquivo alvo {scraped_file_path}: {e}")
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
            logging.error("O carregamento do dicionário de pistas falhou. Operação abortada.")
            return

        logging.info("Garantindo estabilidade do ChromeDriver via WDM...")
        driver_path = ChromeDriverManager().install()

        while True:
            trabalho = db.query(BackfillLog).filter(
                BackfillLog.status.in_(['pending', 'failed'])
            ).order_by(BackfillLog.data_corrida.asc()).first()

            if not trabalho:
                logging.info("Backfill e fila de pendências integralmente concluídos.")
                break
            
            data_alvo = trabalho.data_corrida
            logging.info(f"Fila: Processando data {data_alvo} (Status anterior: {trabalho.status})")
            trabalho.status = 'running'
            trabalho.last_updated = func.now()
            db.commit()

            try:
                run_extraction_for_date(data_alvo, mapa_pistas, driver_path)
                trabalho.status = 'success'
                logging.info(f"Data {data_alvo}: Pipeline concluído e validado.")
                db.commit()

                verificar_links_do_dia(data_alvo.strftime('%Y-%m-%d'))

            except Exception as e:
                logging.error(f"Quebra sistêmica ao processar a data {data_alvo}", exc_info=True)
                db.rollback()
                trabalho.status = 'failed'
                db.commit()
                time.sleep(10)
                
    finally:
        db.close()

if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    formatter = TaxonomiaBackfillFormatter()
    
    fh = logging.FileHandler("backfill_extracao.log", mode='a', encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    logging.getLogger("undetected_chromedriver").setLevel(logging.ERROR)
    logging.getLogger("selenium").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("wdm").setLevel(logging.ERROR)
    
    rodar_backfill()