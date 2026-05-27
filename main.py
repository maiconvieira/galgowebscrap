import os
import logging
from datetime import date
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor

from app.core import config
from app.scrapers import tf_scraper, gh_scraper
from app.processing.processador import processar_e_salvar_dados

from app.core.helpers import (
    obter_data_alvo, 
    inicializar_banco_seguro, 
    carregar_mapa_pistas
)
from app.core.driver_factory import (
    configurar_driver_uc, 
    configurar_driver_padrao
)
from app.core.pipeline_utils import executar_pipeline_site

def run_etl_for_date(target_date: date, mapa_pistas: dict, driver_path: str):
    logging.info(f"### INICIANDO ETL COMPLETO PARA A DATA: {target_date.strftime('%Y-%m-%d')} ###")
    
    data_str = target_date.strftime('%Y-%m-%d')
    arquivo_cache_tf = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_tf.json")
    arquivo_cache_gh = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_gh.json")

    if os.path.exists(arquivo_cache_tf) and os.path.exists(arquivo_cache_gh):
        logging.info(f"Cache JSON (TF e GH) encontrado para {data_str}. Pulando Extração.")
    else:
        logging.info(f"Cache JSON NÃO encontrado para {data_str}. Iniciando Extração.")
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix='Worker') as executor:

            # Submete a Tarefa 1 (Timeform)
            future_tf = executor.submit(
                executar_pipeline_site,
                "Timeform",
                f"{data_str}_links_2_tf.json",
                "2_tf",
                tf_scraper.extrair_links_tf,
                tf_scraper.raspar_detalhes_pagina_tf,
                mapa_pistas,
                config.URL_BASE_TF,
                lambda: configurar_driver_uc(driver_path),
                (1, 3),
                target_date,
                False
            )
            # Submete a Tarefa 2 (Greyhound)
            future_gh = executor.submit(
                executar_pipeline_site,
                "Greyhound",
                f"{data_str}_links_2_gh.json",
                "2_gh",
                gh_scraper.extrair_links_gh,
                gh_scraper.raspar_detalhes_pagina_gh,
                mapa_pistas,
                config.URL_BASE_GH,
                lambda: configurar_driver_padrao(driver_path),
                (10, 20),
                target_date,
                True
            )
            future_tf.result()
            future_gh.result()
        logging.info(f"Extração (Scraping) para {data_str} concluída.")

    logging.info(f"Iniciando Processamento (T & L) para {data_str}...")
    try:
        processar_e_salvar_dados(target_date) 
        logging.info(f"Processamento para {data_str} concluído.")
    except Exception as e_process:
        logging.critical(f"Erro fatal no Processamento (T & L) para {data_str}", exc_info=True)
        raise

def main_diario():
    mapa_pistas = carregar_mapa_pistas()
    if not mapa_pistas:
        return

    try:
        inicializar_banco_seguro()
    except Exception:
        logging.critical("Falha ao inicializar o banco. Abortando execução diária.")
        return

    driver_path = None
    try:
        driver_path = ChromeDriverManager().install()
    except Exception as e_wm:
        logging.critical(f"Falha CRÍTICA ao inicializar o WebDriverManager: {e_wm}", exc_info=True)
        return

    data_alvo = obter_data_alvo()

    try:
        logging.info(f"### === INICIANDO CICLO ETL DIÁRIO PARA {data_alvo} === ###")
        run_etl_for_date(data_alvo, mapa_pistas, driver_path)
        logging.info(f"### === CICLO ETL DIÁRIO PARA {data_alvo} CONCLUÍDO === ###")
    except Exception as e:
        logging.critical(f"### !!! FALHA NO CICLO ETL DIÁRIO para {data_alvo} !!! ###", exc_info=True)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("scanner.log", mode='w'),
            #logging.StreamHandler()
        ]
    )
    main_diario()