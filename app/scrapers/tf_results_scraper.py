import logging
import re
import time
from bs4 import BeautifulSoup
import random # Boa prática adicionar um pouco de "jitter"

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

from app.core import config
from app.core.helpers import analisar_posicao_final

def extrair_links_tf_results(driver, data_para_buscar):
    """
    Busca os links de *resultados* do Timeform para uma data específica.
    """
    data_str = data_para_buscar.strftime('%Y-%m-%d')
    url_resultados = f"{config.URL_BASE_TF}/greyhound-racing/results/{data_str}"
    logging.info(f"[TF-Results] Acessando Timeform para a data de resultados: {data_str}")

    try:
        driver.get(url_resultados)
        # A estrutura da página de resultados é similar à de racecards
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "wfr-bytrack-content")))
    except TimeoutException:
        logging.error(f"[TF-Results] A página de resultados {url_resultados} não carregou a lista a tempo.")
        return []

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    div_principal = soup.find('div', class_='wfr-bytrack-content wfr-content')
    if not div_principal:
        logging.warning("[TF-Results] A estrutura principal da página de resultados Timeform não foi encontrada.")
        return []

    unidades_de_trabalho = []
    
    # A lógica de extração de links é idêntica à do scraper de cards
    for dados_pista in div_principal.find_all('li'):
        for corrida_link in dados_pista.find_all('a'):
            href = corrida_link.get('href')
            if href:
                unidades_de_trabalho.append({
                    'href_tf': href
                })
    
    logging.info(f"-> [TF-Results] {len(unidades_de_trabalho)} links de resultados encontrados no Timeform.")
    return unidades_de_trabalho

def raspar_detalhes_pagina_tf_results(driver, trabalho: dict, mapa_json, max_retries: int = 3):
    """
    Raspa os dados de resultado de uma página de corrida específica do Timeform.
    Foco: Posição Final, SP, Tempo.
    """
    url_completa = f"{config.URL_BASE_TF}{trabalho['href_tf']}"
    logging.info(f"[TF-Results] Raspando resultados de: {url_completa}")

    for attempt in range(max_retries):
        try:
            driver.get(url_completa)
            wait = WebDriverWait(driver, 20)

            # Esperamos pelo contêiner principal do resultado
            # Nota: É diferente do scraper de cards, que espera por 'table.recent-form'
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "result-list-wrapper")))
            
            # Adiciona um "sleep" aleatório, boa prática
            time.sleep(random.uniform(0.5, 1.5))

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Lógica de detecção de bloqueio, crucial, copiada do tf_scraper.py
            body = soup.find('body')
            if body:
                body_content = body.get_text(strip=True)
                block_page_text = 'For data, please visit https://www.globalsportsapi.com/'
                if block_page_text in body_content:
                    logging.warning(f"[TF-Results] Página de bloqueio (GSA) detectada em {url_completa}. Acionando retentativa...")
                    raise TimeoutException("Página de bloqueio (GSA) detectada.")

            dados_da_corrida = {}
            lista_resultados = []

            container_resultados = soup.find('div', class_='result-list-wrapper')
            if not container_resultados:
                logging.error(f"[TF-Results] Contêiner de resultados 'result-list-wrapper' não encontrado em {url_completa}.")
                return None # Falha 'soft' para não tentar novamente

            # Os itens de resultado estão em 'result-list-item'
            for item in container_resultados.find_all('div', class_='result-list-item'):
                try:
                    dados_participante = {}

                    # 1. Posição Final
                    pos_el = item.find('span', class_='finishing-position')
                    pos_text = pos_el.get_text(strip=True) if pos_el else None
                    dados_participante['posicao_final'] = analisar_posicao_final(pos_text)

                    # 2. Faixa (Trap)
                    faixa_el = item.find('span', class_='trap-number')
                    dados_participante['faixa'] = int(faixa_el.get_text(strip=True)) if faixa_el else None

                    # 3. Nome (para validação)
                    nome_el = item.find('a', class_='runner-name')
                    dados_participante['nome_galgo'] = nome_el.get_text(strip=True).title() if nome_el else None
                    
                    # 4. SP (Starting Price)
                    sp_el = item.find('span', class_='starting-price')
                    dados_participante['sp_real'] = sp_el.get_text(strip=True) if sp_el else None
                    
                    # 5. Tempo Real (Calculated Time)
                    # Nota: O Timeform nem sempre fornece o tempo individual, 
                    # mas sim o 'calculated-time'
                    tempo_el = item.find('span', class_='calculated-time')
                    if tempo_el and tempo_el.get_text(strip=True):
                        dados_participante['tempo_final_real'] = float(tempo_el.get_text(strip=True))
                    else:
                        dados_participante['tempo_final_real'] = None

                    if dados_participante['faixa']: # Só salva se tivermos a faixa
                        lista_resultados.append(dados_participante)

                except Exception as e_parse:
                    logging.warning(f"[TF-Results] Erro ao parsear um participante em {url_completa}. Pulando item.", exc_info=True)
                    continue
            
            dados_da_corrida['participantes_resultado'] = lista_resultados
            # Mescla 'href_tf' com os dados raspados
            dados_completos = {**trabalho, **dados_da_corrida}
            
            return dados_completos

        except (TimeoutException, NoSuchElementException) as e:
            # Reutiliza a lógica de retry exata do tf_scraper.py
            logging.warning(f"[TF-Results] Tentativa {attempt + 1}/{max_retries} falhou para {url_completa}. Erro: {type(e).__name__}.")
            if attempt < max_retries - 1:
                sleep_time = (attempt + 1) * 2
                logging.info(f"Aguardando {sleep_time} segundos antes de tentar novamente.")
                time.sleep(sleep_time)
                try:
                    driver.refresh()
                except WebDriverException as e_refresh:
                    logging.error(f"[TF-Results] Falha ao dar refresh no driver. Abortando retentativas.", exc_info=True)
                    raise e_refresh
            else:
                logging.error(f"[TF-Results] Todas as {max_retries} tentativas falharam para {url_completa}. Descartando tarefa.")
                return None
    
    logging.error(f"[TF-Results] Falha ao raspar {url_completa} após {max_retries} tentativas. Retornando None.")
    return None