import os

pasta_cache_auditoria = os.path.join(os.getcwd(), 'wdm_cache_auditoria')
os.environ['WDM_DIR'] = pasta_cache_auditoria

import json
import logging
from datetime import date, timedelta
from webdriver_manager.chrome import ChromeDriverManager
from app.core.driver_factory import configurar_driver_padrao
from app.scrapers.gh_arquivo_scraper import extrair_links_gh
from app.scrapers.tf_arquivo_scraper import extrair_links_tf

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("auditoria_links.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def comparar_sets(links_site, links_arquivo, descricao, data):
    if not links_site:
        site_set = set()
    else:
        site_set = {c['href_gh'] for c in links_site} if 'href_gh' in links_site[0] else {c['href_tf'] for c in links_site}
        
    if not links_arquivo:
        arquivo_set = set()
    else:
        arquivo_set = {c['href_gh'] for c in links_arquivo} if 'href_gh' in links_arquivo[0] else {c['href_tf'] for c in links_arquivo}
    
    dif_site_arquivo = site_set - arquivo_set
    dif_arquivo_site = arquivo_set - site_set
    
    if dif_site_arquivo or dif_arquivo_site:
        logging.warning(f"--- DIVERGÊNCIA EM {descricao} ({data}) ---")
        if dif_site_arquivo:
            logging.warning(f"  [+] No Site, mas NÃO no Arquivo: {len(dif_site_arquivo)} itens.")
        if dif_arquivo_site:
            logging.warning(f"  [-] No Arquivo, mas NÃO no Site: {len(dif_arquivo_site)} itens.")
    else:
        logging.info(f"[{descricao} ({data})] OK: 100% de paridade.")

def auditar_periodo(data_inicio, data_fim, driver_path):
    logging.info(f"Iniciando auditoria de {data_inicio} até {data_fim}...")

    driver = configurar_driver_padrao(driver_path)
    delta = data_fim - data_inicio

    try:
        for i in range(delta.days + 1):
            data_alvo = data_inicio + timedelta(days=i)
            data_str = data_alvo.strftime('%Y-%m-%d')

            caminho_gh = f"data/{data_str}_links_1_gh.json"
            caminho_tf = f"data/{data_str}_links_1_tf.json"
            
            existe_gh = os.path.exists(caminho_gh)
            existe_tf = os.path.exists(caminho_tf)

            if not existe_gh and not existe_tf:
                logging.info(f"[{data_str}] IGNORADO: Nenhum arquivo JSON local encontrado.")
                continue
                
            logging.info(f"--- Analisando data: {data_str} ---")

            if existe_gh:
                with open(caminho_gh, 'r') as f:
                    links_gh_arquivo = json.load(f).get('corridas', [])
                
                links_gh_site = extrair_links_gh(driver, data_alvo)
                comparar_sets(links_gh_site, links_gh_arquivo, "Greyhound Bet", data_str)
            else:
                logging.warning(f"[Greyhound Bet ({data_str})] Arquivo JSON não existe. Análise pulada.")

            if existe_tf:
                with open(caminho_tf, 'r') as f:
                    links_tf_arquivo = json.load(f).get('corridas', [])
                
                links_tf_site = extrair_links_tf(driver, data_alvo)
                comparar_sets(links_tf_site, links_tf_arquivo, "Timeform", data_str)
            else:
                logging.warning(f"[Timeform ({data_str})] Arquivo JSON não existe. Análise pulada.")
                
    except Exception as e:
        logging.error(f"Ocorreu um erro fatal durante a iteração: {e}", exc_info=True)
    finally:
        driver.quit()
        logging.info("Auditoria concluída e navegador fechado.")

if __name__ == "__main__":
    print("Obtendo caminho do ChromeDriver...")
    caminho_driver = ChromeDriverManager().install()

    data_inicial = date(2022, 1, 1)
    data_final = date(2022, 12, 31)
    #data_final = date.today() 
    
    auditar_periodo(data_inicial, data_final, caminho_driver)