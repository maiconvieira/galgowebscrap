import os
import time
import json
import random
import logging
from datetime import date

from selenium.common.exceptions import WebDriverException, TimeoutException
from urllib3.exceptions import ProtocolError, MaxRetryError

from app.core import config
from app.core.driver_factory import warm_up_driver
from app.core.helpers import salvar_dados_em_json

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
        with open(caminho_tmp, 'w', encoding='utf-8') as f:
            json.dump(novo_cache, f, indent=2)
            
        if os.path.exists(caminho_do_cache):
            try:
                os.remove(caminho_do_cache)
            except OSError:
                logging.warning(f"Arquivo {caminho_do_cache} travado por outro processo. Tentando forçar substituição.")
                
        os.replace(caminho_tmp, caminho_do_cache)
        logging.info(f"-> Novos links ({len(lista_de_corridas)}) salvos em '{caminho_do_cache}' para a data {data_alvo_str}.")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo de cache '{caminho_do_cache}'.", exc_info=True)
        if os.path.exists(caminho_tmp):
            os.remove(caminho_tmp)

    return lista_de_corridas

def processar_lista_em_serie(lista_de_trabalho: list, funcao_raspagem: callable, mapa_json, url_base: str, driver_factory: callable, pausa_config: tuple):
    if not lista_de_trabalho:
        logging.info("Lista de trabalho vazia. Nenhum processamento necessário.")
        return []

    resultados = []
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

    logging.info(f"Processamento serial concluído. {len(resultados)} resultados coletados.")
    return resultados

def executar_pipeline_site(
    nome_fonte: str,
    nome_cache_links: str,
    sufixo_dados: str,
    funcao_extracao: callable,
    funcao_raspagem: callable,
    mapa_json,
    url_base: str,
    driver_factory: callable,
    pausa_config: tuple,
    data_alvo: date
):
    try:
        logging.info(f"### INICIANDO PIPELINE {nome_fonte.upper()} PARA {data_alvo.strftime('%Y-%m-%d')} ###")
        lista_links = cache_links(nome_cache_links, funcao_extracao, driver_factory, data_alvo)
        
        lista_de_trabalho_final = lista_links
        chave_href = f"href_{sufixo_dados.split('_')[-1]}"

        data_alvo_str = data_alvo.strftime('%Y-%m-%d')
        nome_arquivo_scraped = f"{data_alvo_str}_scraped_{sufixo_dados}.json"
        caminho_arquivo_scraped = os.path.join(config.PASTA_DE_DADOS, nome_arquivo_scraped)

        if os.path.exists(caminho_arquivo_scraped):
            try:
                with open(caminho_arquivo_scraped, 'r', encoding='utf-8') as f:
                    dados_raspados_existentes = json.load(f)

                links_ja_raspados = {
                    corrida.get(chave_href) 
                    for corrida in dados_raspados_existentes 
                    if isinstance(corrida, dict) and corrida.get(chave_href)
                }
                
                if links_ja_raspados:
                    lista_de_trabalho_final = [
                        trabalho for trabalho in lista_links 
                        if trabalho.get(chave_href) not in links_ja_raspados
                    ]
                    logging.info(f"-> {nome_fonte}: Arquivo raspado parcial encontrado.")
                    logging.info(f"-> {nome_fonte}: Links de origem: {len(lista_links)}. Já raspados: {len(links_ja_raspados)}. Faltando: {len(lista_de_trabalho_final)}.")
                
            except (json.JSONDecodeError, Exception) as e:
                logging.warning(f"Não foi possível ler o arquivo raspado parcial '{caminho_arquivo_scraped}'. O scraping será re-executado.", exc_info=True)
        
        dados_scraped = processar_lista_em_serie(lista_links, funcao_raspagem, mapa_json, url_base, driver_factory, pausa_config)
        salvar_dados_em_json(dados_scraped, nome_fonte, sufixo_dados, data_alvo)
        logging.info(f"### PIPELINE {nome_fonte.upper()} CONCLUÍDO PARA {data_alvo.strftime('%Y-%m-%d')} ###")
    except Exception as e:
        logging.critical(f"!!! ERRO FATAL NO PIPELINE {nome_fonte.upper()} !!!", exc_info=True)
        raise