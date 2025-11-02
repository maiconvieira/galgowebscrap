import os
import time
import json
import random
import logging
import threading
import undetected_chromedriver as uc
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor

from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from urllib3.exceptions import ProtocolError, MaxRetryError, NewConnectionError
from app.core import config
from app.db.conexao import engine, Base
from app.db import modelos
from app.core.helpers import obter_data_alvo, salvar_dados_em_json
from app.scrapers import tf_scraper, gh_scraper
from app.processing.processador import processar_e_salvar_dados

driver_creation_lock = threading.Lock()

def configurar_driver_uc(driver_path: str):
    with driver_creation_lock:
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        options.add_argument(f'--user-agent={USER_AGENT}')
        
        try:
            driver = uc.Chrome(options=options, 
                               use_subprocess=True,
                               driver_executable_path=driver_path)

            cooldown = random.uniform(5, 10)
            time.sleep(cooldown)
            return driver
        except Exception as e:
            logging.critical("Falha CRÍTICA ao criar a instância do driver (Undetected).", exc_info=True)
            logging.error("Liberando lock após falha na criação do driver.")
            return None

def configurar_driver_padrao(driver_path: str):
    with driver_creation_lock:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        options.add_argument(f'--user-agent={USER_AGENT}')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        try:
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=options)
            cooldown = random.uniform(5, 10)
            time.sleep(cooldown)
            return driver
        except Exception as e:
            logging.critical("Falha CRÍTICA ao criar a instância do driver (Selenium Padrão).", exc_info=True)
            logging.error("Liberando lock após falha na criação do driver.")
            return None

def cache_links(nome_cache: str, funcao_extracao: callable, driver_factory: callable, data_alvo: date, max_retries: int = 3):
    os.makedirs(config.PASTA_DE_DADOS, exist_ok=True)
    caminho_do_cache = os.path.join(config.PASTA_DE_DADOS, nome_cache)
    data_alvo_str = data_alvo.strftime('%Y-%m-%d')

    if os.path.exists(caminho_do_cache):
        try:
            with open(caminho_do_cache, 'r') as f:
                dados_cache = json.load(f)

            if dados_cache.get('data_corrida') == data_alvo_str:
                logging.info(f"-> Cache de links encontrado e válido para {data_alvo_str} em '{nome_cache}'.")
                return dados_cache.get('corridas', [])
            else:
                logging.warning(f"-> Cache de '{caminho_do_cache}' é de outra data. Será regerado.")

        except (json.JSONDecodeError, KeyError):
            logging.warning(f"-> Cache de '{caminho_do_cache}' corrompido ou mal formatado. Será regerado.")

    logging.info(f"Iniciando busca de links web para '{nome_cache}' (Data: {data_alvo_str})...")
    lista_de_corridas = []

    for attempt in range(max_retries):
        driver_para_links = None
        logging.info(f"Tentativa {attempt + 1}/{max_retries} para buscar links de '{nome_cache}'...")
        try:
            driver_para_links = driver_factory()
            if not driver_para_links:
                logging.critical(f"Tentativa {attempt + 1}: Não foi possível criar o driver. Aguardando para tentar novamente.")
                time.sleep(random.uniform(0, 3))

            lista_de_corridas = funcao_extracao(driver_para_links, data_alvo)

            logging.info(f"Tentativa {attempt + 1} bem-sucedida. {len(lista_de_corridas)} links encontrados.")
            break

        except Exception as e:
            logging.error(f"Tentativa {attempt + 1}/{max_retries} falhou ao extrair links para '{nome_cache}'.", exc_info=True)
            if attempt < max_retries - 1:
                sleep_time = (attempt + random.uniform(0, 3)) * 2
                logging.info(f"Aguardando {sleep_time}s antes de tentar novamente...")
                time.sleep(sleep_time)
        finally:
            if driver_para_links:
                driver_para_links.quit()
                logging.info(f"Driver temporário (tentativa {attempt + 1}) para '{nome_cache}' fechado.")

    if not lista_de_corridas:
        logging.error(f"Todas as {max_retries} tentativas de extrair links para '{nome_cache}' falharam.")
        return []

    novo_cache = {
        'data_corrida': data_alvo_str,
        'corridas': lista_de_corridas
    }
    try:
        caminho_tmp = caminho_do_cache + ".tmp"
        with open(caminho_tmp, 'w') as f:
            json.dump(novo_cache, f, indent=2)
        os.replace(caminho_tmp, caminho_do_cache)
        logging.info(f"-> Novos links ({len(lista_de_corridas)}) salvos em '{caminho_do_cache}' para a data {data_alvo_str}.")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo de cache '{caminho_do_cache}'.", exc_info=True)
        if os.path.exists(caminho_tmp):
            os.remove(caminho_tmp)

    return lista_de_corridas

def processar_lista_em_serie(lista_de_trabalho: list, funcao_raspagem: callable, mapa_json, url_base: str, driver_factory: callable, pausa_config: tuple, restart_por_trabalho: bool = False):
    if not lista_de_trabalho:
        logging.info("Lista de trabalho vazia. Nenhum processamento necessário.")
        return []

    resultados = []

    if not restart_por_trabalho:
        driver = None
        try:
            driver = driver_factory()   
            driver = warm_up_driver(driver, url_base)
            if not driver:
                logging.critical("Não foi possível criar o driver principal. Abortando processamento serial.")
                return []

            total_trabalhos = len(lista_de_trabalho)
            logging.info(f"Iniciando processamento serial (modo driver único) de {total_trabalhos} trabalhos...")

            for i, trabalho in enumerate(lista_de_trabalho):
                url_trabalho = trabalho.get('href_tf') or trabalho.get('href_gh')
                logging.info(f"Processando [{i+1}/{total_trabalhos}]: {url_trabalho}")

                try:
                    resultado = funcao_raspagem(driver, trabalho, mapa_json)
                    
                    if resultado:
                        resultados.append(resultado)
                    else:
                        logging.warning(f"Scraping de {url_trabalho} não retornou dados (possivelmente falhou após retentativas).")

                except (WebDriverException, MaxRetryError, ProtocolError, TimeoutException, AttributeError) as e_driver_comm:
                    logging.critical(f"ERRO CRÍTICO DE DRIVER/COMUNICAÇÃO ({type(e_driver_comm).__name__}) ao processar {url_trabalho}.", exc_info=True)
                    logging.error("Tentando reiniciar o driver...")
                    try:
                        driver.quit()
                    except:
                        pass 
                    
                    driver = driver_factory()
                    driver = warm_up_driver(driver, url_base)

                    if not driver:
                        logging.critical("Não foi possível reiniciar o driver. Abortando os trabalhos restantes.")
                        break 

                except Exception as e_parse:
                    logging.error(f"Erro inesperado de scraping/parsing ao processar {url_trabalho}", exc_info=True)
                
                pausa_polida = random.uniform(pausa_config[0], pausa_config[1])
                logging.info(f"Pausa de {pausa_polida:.1f}s...")
                time.sleep(pausa_polida)
        finally:
            if driver:
                logging.info("Fechando o driver principal (modo driver único) após concluir os trabalhos.")
                driver.quit()

    else:
        total_trabalhos = len(lista_de_trabalho)
        logging.info(f"Iniciando processamento serial (modo 'um-driver-por-trabalho') de {total_trabalhos} trabalhos...")

        for i, trabalho in enumerate(lista_de_trabalho):
            url_trabalho = trabalho.get('href_tf') or trabalho.get('href_gh')
            logging.info(f"Processando [{i+1}/{total_trabalhos}]: {url_trabalho}")

            driver = None
            try:
                driver = driver_factory()
                driver = warm_up_driver(driver, url_base)
                if not driver:
                    logging.error(f"Não foi possível criar o driver para o trabalho {url_trabalho}. Pulando.")
                    continue

                resultado = funcao_raspagem(driver, trabalho, mapa_json)
                
                if resultado:
                    resultados.append(resultado)
                else:
                    logging.warning(f"Scraping de {url_trabalho} não retornou dados.")

            except (WebDriverException, MaxRetryError, ProtocolError, TimeoutException, AttributeError) as e_driver_comm:
                logging.critical(f"ERRO CRÍTICE DE DRIVER/COMUNICAÇÃO ({type(e_driver_comm).__name__}) no trabalho {url_trabalho}. O driver será descartado.", exc_info=True)
            except Exception as e_parse:
                logging.error(f"Erro inesperado de scraping/parsing ao processar {url_trabalho}", exc_info=True)
            
            finally:
                if driver:
                    driver.quit()
                logging.info(f"Driver do trabalho [{i+1}/{total_trabalhos}] fechado. Próxima iteração.")

    logging.info(f"Processamento serial concluído. {len(resultados)} resultados coletados.")
    return resultados

def warm_up_driver(driver_instance, url_base: str):
    if not driver_instance:
        return None
    try:
        logging.info(f"Aquecendo o driver com a URL base: {url_base}")
        driver_instance.get(url_base)
        time.sleep(random.uniform(4, 7))
        if "timeform" in url_base:
            try:
                cookie_wait = WebDriverWait(driver_instance, 5)
                accept_button = cookie_wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                accept_button.click()
                time.sleep(1)
            except TimeoutException:
                logging.info("Banner de cookies (Timeform) não encontrado durante o aquecimento. Seguindo.")
            except Exception as e_cookie:
                logging.warning(f"Erro ao tentar aceitar cookies no aquecimento: {e_cookie}")

        return driver_instance
    except (WebDriverException, MaxRetryError, ProtocolError) as e_warmup:
        logging.critical(f"FALHA NO AQUECIMENTO do driver ({type(e_warmup).__name__}). Tentando reiniciar...", exc_info=True)
        try:
            driver_instance.quit()
        except:
            pass
        return None

def executar_pipeline_site(
    nome_fonte: str, 
    nome_cache_links: str,  # <-- Renomeado para clareza
    sufixo_dados: str,      # <-- NOVO PARÂMETRO
    funcao_extracao: callable, 
    funcao_raspagem: callable, 
    mapa_json, 
    url_base: str, 
    driver_factory: callable, 
    pausa_config: tuple, 
    data_alvo: date, 
    restart_por_trabalho: bool = False
):
    try:
        logging.info(f"### INICIANDO PIPELINE {nome_fonte.upper()} PARA {data_alvo.strftime('%Y-%m-%d')} ###")
        lista_links = cache_links(nome_cache_links, funcao_extracao, driver_factory, data_alvo)
        dados_scraped = processar_lista_em_serie(lista_links, funcao_raspagem, mapa_json, url_base, driver_factory, pausa_config, restart_por_trabalho)
        salvar_dados_em_json(dados_scraped, nome_fonte, sufixo_dados, data_alvo)
        logging.info(f"### PIPELINE {nome_fonte.upper()} CONCLUÍDO PARA {data_alvo.strftime('%Y-%m-%d')} ###")
    except Exception as e:
        logging.critical(f"!!! ERRO FATAL NO PIPELINE {nome_fonte.upper()} !!!", exc_info=True)
        raise

def run_etl_for_date(target_date: date, mapa_pistas: dict, driver_path: str):
    logging.info(f"### INICIANDO ETL COMPLETO PARA A DATA: {target_date.strftime('%Y-%m-%d')} ###")
    
    data_str = target_date.strftime('%Y-%m-%d')
    arquivo_cache_tf = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_tf.json")
    arquivo_cache_gh = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_gh.json")

    if os.path.exists(arquivo_cache_tf) and os.path.exists(arquivo_cache_gh):
        logging.info(f"Cache JSON (TF e GH) encontrado para {data_str}. Pulando Extração (Scraping).")
    else:
        logging.info(f"Cache JSON NÃO encontrado para {data_str}. Iniciando Extração (Scraping).")
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix='Worker') as executor:

            # Submete a Tarefa 1 (Timeform)
            future_tf = executor.submit(
                executar_pipeline_site,
                "Timeform",
                f"{data_str}_links_tf.json",
                "tf",
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
                f"{data_str}_links_gh.json",
                "gh",
                gh_scraper.extrair_links_gh,
                gh_scraper.raspar_detalhes_pagina_gh,
                mapa_pistas,
                config.URL_BASE_GH,
                lambda: configurar_driver_padrao(driver_path),
                (18, 23),
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

def inicializar_banco_seguro():
    try:
        logging.info("Verificando e criando tabelas (se não existirem)...")
        Base.metadata.create_all(bind=engine, checkfirst=True)
        logging.info("Banco de dados verificado com sucesso.")
    except Exception as e:
        logging.critical(f"Não foi possível inicializar o banco de dados. Abortando.", exc_info=True)
        raise

def carregar_mapa_pistas():
    try:
        with open(config.ARQUIVO_MAPA_PISTAS, 'r') as f:
            mapa_pistas = json.load(f)
            logging.info(f"Mapa de pistas '{config.ARQUIVO_MAPA_PISTAS}' carregado.")
            return mapa_pistas
    except FileNotFoundError:
        logging.critical(f"Arquivo '{config.ARQUIVO_MAPA_PISTAS}' não encontrado. Abortando.")
        return None
    except json.JSONDecodeError:
        logging.critical(f"Arquivo '{config.ARQUIVO_MAPA_PISTAS}' está corrompido (JSON inválido). Abortando.")
        return None

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
        logging.info("Pré-instalando/verificando o ChromeDriver...")
        driver_path = ChromeDriverManager().install()
        logging.info(f"ChromeDriver está pronto em: {driver_path}")
    except Exception as e_wm:
        logging.critical(f"Falha CRÍTICA ao inicializar o WebDriverManager: {e_wm}", exc_info=True)
        return

    data_alvo = obter_data_alvo()

    try:
        logging.info(f"### === INICIANDO CICLO ETL DIÁRIO PARA {data_alvo} === ###")
        run_etl_for_date(data_alvo, mapa_pistas)
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