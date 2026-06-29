import os
import json
import logging
from datetime import date, timedelta

pasta_cache_backfill = os.path.join(os.getcwd(), 'wdm_cache_backfill')
os.environ['WDM_DIR'] = pasta_cache_backfill

from webdriver_manager.chrome import ChromeDriverManager
from app.core.driver_factory import configurar_driver_padrao
from app.scrapers.gh_arquivo_scraper import raspar_detalhes_pagina_gh
from app.scrapers.tf_arquivo_scraper import raspar_detalhes_pagina_tf

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def executar_recuperacao_incremental(diretorio_data, data_inicio, data_fim, caminho_do_driver):
    delta = data_fim - data_inicio
    driver = None

    try:
        for i in range(delta.days + 1):
            data_alvo = data_inicio + timedelta(days=i)
            data_str = data_alvo.strftime('%Y-%m-%d')

            for sufixo, chave, func_raspagem in [
                ("gh", "href_gh", raspar_detalhes_pagina_gh), 
                ("tf", "href_tf", raspar_detalhes_pagina_tf)
            ]:
                arq_links = os.path.join(diretorio_data, f"{data_str}_links_1_{sufixo}.json")
                arq_scraped_1 = os.path.join(diretorio_data, f"{data_str}_scraped_1_{sufixo}.json")
                arq_scraped_2 = os.path.join(diretorio_data, f"{data_str}_scraped_2_{sufixo}.json")

                if not os.path.exists(arq_links) or not os.path.exists(arq_scraped_1):
                    continue

                with open(arq_links, 'r', encoding='utf-8') as f:
                    dados_links = json.load(f).get('corridas', [])
                
                with open(arq_scraped_1, 'r', encoding='utf-8') as f:
                    dados_scraped_1 = json.load(f)

                set_scraped = {corrida[chave] for corrida in dados_scraped_1 if chave in corrida}

                dados_scraped_2 = []
                if os.path.exists(arq_scraped_2):
                    with open(arq_scraped_2, 'r', encoding='utf-8') as f:
                        dados_scraped_2 = json.load(f)
                    set_scraped.update(corrida[chave] for corrida in dados_scraped_2 if chave in corrida)

                set_links = {corrida[chave] for corrida in dados_links if chave in corrida}
                urls_faltantes = set_links - set_scraped

                if urls_faltantes:
                    logging.info(f"[{data_str} | {sufixo.upper()}] Recuperando {len(urls_faltantes)} itens faltantes para o arquivo 2.")
                    
                    if driver is None:
                        driver = configurar_driver_padrao(caminho_do_driver)

                    novos_registros = []
                    
                    for url in urls_faltantes:
                        trabalho_simulado = {chave: url}
                        resultado = func_raspagem(driver, trabalho_simulado, None, max_retries=3)
                        
                        if resultado:
                            novos_registros.append(resultado)
                            logging.info(f"Recuperação bem-sucedida: {url}")
                        else:
                            logging.error(f"Falha definitiva na recuperação: {url}")

                    if novos_registros:
                        registros_consolidados = dados_scraped_2 + novos_registros
                        
                        with open(arq_scraped_2, 'w', encoding='utf-8') as f:
                            json.dump(registros_consolidados, f, indent=4, ensure_ascii=False, default=str)
                        
                        logging.info(f"[{data_str} | {sufixo.upper()}] Arquivo secundário criado/atualizado com {len(novos_registros)} novos itens.")

    finally:
        if driver is not None:
            driver.quit()

if __name__ == "__main__":
    print("Obtendo caminho do ChromeDriver isolado para backfill...")
    caminho_driver = ChromeDriverManager().install()

    PASTA_DADOS = "./data"
    #DATA_DE_INICIO = date(2021, 1, 25)
    #DATA_DE_FIM = date(2021, 1, 27)
    DATA_DE_INICIO = DATA_DE_FIM = date(2024, 9, 13)

    executar_recuperacao_incremental(PASTA_DADOS, DATA_DE_INICIO, DATA_DE_FIM, caminho_driver)
    logging.info("Rotina de recuperação incremental finalizada.")