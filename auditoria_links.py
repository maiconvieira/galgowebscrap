import os
os.environ['WDM_LOG'] = '0'
os.environ['WDM_PRINT_FIRST_LINE'] = 'False'

import json
import logging
import argparse
from datetime import date, timedelta, datetime
from webdriver_manager.chrome import ChromeDriverManager
from app.core.driver_factory import configurar_driver_padrao
from app.scrapers.gh_arquivo_scraper import extrair_links_gh
from app.scrapers.tf_arquivo_scraper import extrair_links_tf

pasta_cache_auditoria = os.path.join(os.getcwd(), 'wdm_cache_auditoria')
os.environ['WDM_DIR'] = pasta_cache_auditoria

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("auditoria_links.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def comparar_sets(links_site, links_arquivo, modulo, data):
    site_set = set()
    if links_site:
        site_set = {c['href_gh'] for c in links_site} if 'href_gh' in links_site[0] else {c['href_tf'] for c in links_site}
        
    arquivo_set = set()
    if links_arquivo:
        arquivo_set = {c['href_gh'] for c in links_arquivo} if 'href_gh' in links_arquivo[0] else {c['href_tf'] for c in links_arquivo}
    
    dif_site_arquivo = site_set - arquivo_set
    dif_arquivo_site = arquivo_set - site_set
    
    if dif_site_arquivo or dif_arquivo_site:
        log_singular(data, modulo, "WARN", "Divergência de paridade detectada.")
        if dif_site_arquivo:
            log_singular(data, modulo, "WARN", f"[+] No Site, mas NÃO no Arquivo: {len(dif_site_arquivo)} itens.")
        if dif_arquivo_site:
            log_singular(data, modulo, "WARN", f"[-] No Arquivo, mas NÃO no Site: {len(dif_arquivo_site)} itens.")
    else:
        log_singular(data, modulo, "OK", "Paridade de links validada (100%).")

def log_singular(data_str, modulo, status, mensagem):
    logging.info(f"[{data_str}][{modulo:<3}][{status:<4}] {mensagem}")

def auditar_periodo(data_inicio, data_fim, driver_path):
    driver = configurar_driver_padrao(driver_path)
    delta = data_fim - data_inicio

    for i in range(delta.days + 1):
        data_alvo = data_inicio + timedelta(days=i)
        data_str = data_alvo.strftime('%Y-%m-%d')
        
        if data_alvo.day == 25 and data_alvo.month == 12:
            log_singular(data_str, "SYS", "INFO", "Feriado operacional (Natal); auditoria suprimida.")
            continue

        try:
            caminho_gh = f"data/{data_str}_links_1_gh.json"
            caminho_tf = f"data/{data_str}_links_1_tf.json"
            
            existe_gh = os.path.exists(caminho_gh)
            existe_tf = os.path.exists(caminho_tf)

            if not existe_gh and not existe_tf:
                log_singular(data_str, "SYS", "SKIP", "Nenhum arquivo JSON local encontrado.")
                continue

            if existe_gh:
                with open(caminho_gh, 'r') as f:
                    links_gh_arquivo = json.load(f).get('corridas', [])
                links_gh_site = extrair_links_gh(driver, data_alvo)
                comparar_sets(links_gh_site, links_gh_arquivo, "GH", data_str)
            else:
                log_singular(data_str, "GH", "SKIP", "Arquivo JSON local inexistente.")

            if existe_tf:
                with open(caminho_tf, 'r') as f:
                    links_tf_arquivo = json.load(f).get('corridas', [])
                links_tf_site = extrair_links_tf(driver, data_alvo)
                comparar_sets(links_tf_site, links_tf_arquivo, "TF", data_str)
            else:
                log_singular(data_str, "TF", "SKIP", "Arquivo JSON local inexistente.")
            
        except Exception as e:
            log_singular(data_str, "SYS", "ERR", f"Exceção fatal no processamento do dia: {e}")

    driver.quit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Auditoria de Links do Site vs JSON")
    parser.add_argument("--inicio", type=str, required=True, help="Data de início (YYYY-MM-DD)")
    parser.add_argument("--fim", type=str, required=False, help="Data final (YYYY-MM-DD). Opcional, padrão é ontem.")
    args = parser.parse_args()

    ontem = date.today() - timedelta(days=1)

    data_inicial = datetime.strptime(args.inicio, '%Y-%m-%d').date()
    data_final = datetime.strptime(args.fim, '%Y-%m-%d').date() if args.fim else ontem

    logging.info(f"[SYSTEM    ][SYS ][INIT] Obtendo caminho do ChromeDriver...")
    caminho_driver = ChromeDriverManager().install()
    
    auditar_periodo(data_inicial, data_final, caminho_driver)