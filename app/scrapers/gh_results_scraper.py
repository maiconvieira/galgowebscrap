import re
import time
import logging
from bs4 import BeautifulSoup

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, WebDriverException

from app.core import config
from app.core.helpers import analisar_posicao_final

def extrair_links_gh_results(driver, data_para_buscar):
    data_url = data_para_buscar.strftime('%Y-%m-%d')
    url_resultados = f"{config.URL_BASE_GH}#results-list/r_date={data_url}"
    logging.info(f"Acessando Greyhound Bet para a data de resultados: {data_url}")
    
    try:
        driver.get(url_resultados)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.appList.raceList")))
    except TimeoutException:
        logging.error(f"[GH-Results] A página de resultados {url_resultados} não carregou a lista a tempo.")
        return []

    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    ul_principal = soup.find('ul', class_='appList raceList')
    if not ul_principal:
        logging.warning("A estrutura principal da página do Greyhound Bet não foi encontrada.")
        return []

    unidades_de_trabalho = []
    
    li_tags = ul_principal.find_all('li')
    for li in li_tags:
            link_tag = li.find('a')
            if not link_tag or not link_tag.has_attr('href'):
                continue

            href = link_tag['href']
            # Limpa o href para garantir que pouse na página principal
            # A lógica é similar, mas focada em remover 'tab=result' se existir
            if '&tab=result' in href:
                href = href.replace('&tab=result', '')
            if '&tab=form' in href:
                href = href.replace('&tab=form', '')
            if '&tab=card' in href:
                href = href.replace('&tab=card', '')

            unidades_de_trabalho.append({'href_gh': href})
    
    logging.info(f"-> [GH-Results] {len(unidades_de_trabalho)} links de resultados encontrados no Greyhound Bet.")
    return unidades_de_trabalho

def raspar_detalhes_pagina_gh_results(driver, trabalho: dict, mapa_json, max_retries: int = 3):
    """
    Raspa os dados de resultado de uma página de corrida específica do GH.
    Foco: Clicar na aba "Result" e extrair Posição Final, SP, Tempo.
    """
    # Removemos qualquer '&tab=' residual para garantir que o driver controle a navegação
    href_limpo = trabalho['href_gh'].split('&tab=')[0]
    url_completa = f"{config.URL_BASE_GH}{href_limpo}"

    for attempt in range(max_retries):
        try:
            driver.get(url_completa)
            wait = WebDriverWait(driver, 30)
            
            # 1. Espera o contêiner de abas carregar (exatamente como gh_scraper.py)
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.cardTabContainer")))

            # 2. Encontra e clica na aba "Result"
            # (gh_scraper.py clica em "form", nós clicamos em "result")
            result_tab_button = wait.until(EC.element_to_be_clickable((By.ID, "cardTab-result")))
            driver.execute_script("arguments[0].click();", result_tab_button)

            # 3. Espera o contêiner de *resultados* aparecer
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.resultTabContainer")))
            time.sleep(1) # Pausa para garantir que o JS renderizou

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            dados_da_corrida = {}
            lista_resultados = []

            result_container = soup.find('div', class_='resultTabContainer')
            if not result_container:
                logging.error(f"[GH-Results] Contêiner 'resultTabContainer' não encontrado em {url_completa}.")
                return None

            # A tabela de resultados (geralmente 'resultsGrid' ou 'resultsTable')
            tabela_resultados = result_container.find('table', class_=re.compile(r'resultsGrid|resultsTable'))
            if not tabela_resultados or not tabela_resultados.find('tbody'):
                logging.warning(f"[GH-Results] Tabela de resultados não encontrada em {url_completa}.")
                return None

            for linha in tabela_resultados.find('tbody').find_all('tr'):
                try:
                    celulas = linha.find_all('td')
                    if len(celulas) < 8: # Precisa ter colunas suficientes
                        continue

                    dados_participante = {}

                    # 1. Posição Final
                    pos_text = celulas[0].get_text(strip=True)
                    dados_participante['posicao_final'] = analisar_posicao_final(pos_text)

                    # 2. Faixa (Trap)
                    # A lógica de extrair a faixa é complexa no GH, modelada no gh_scraper.py
                    i_tag = celulas[1].find('i', class_=re.compile(r'trap\d+'))
                    if i_tag:
                        for class_name in i_tag['class']:
                            match = re.search(r'trap(\d+)', class_name)
                            if match:
                                dados_participante['faixa'] = int(match.group(1))
                                break
                    
                    if not dados_participante.get('faixa'):
                        logging.warning(f"[GH-Results] Não foi possível encontrar a faixa para {url_completa}. Pulando linha.")
                        continue

                    # 3. Nome (para validação)
                    nome_el = celulas[2].find('a')
                    dados_participante['nome_galgo'] = nome_el.get_text(strip=True).title() if nome_el else None

                    # 4. SP (Starting Price)
                    # A coluna exata pode variar, mas geralmente é após o nome/distância
                    dados_participante['sp_real'] = celulas[5].get_text(strip=True)

                    # 5. Tempo Real (Calc Time)
                    # O tempo calculado é geralmente uma das últimas colunas
                    try:
                        dados_participante['tempo_final_real'] = float(celulas[7].get_text(strip=True))
                    except (ValueError, TypeError):
                        dados_participante['tempo_final_real'] = None
                    
                    lista_resultados.append(dados_participante)

                except Exception as e_parse:
                    logging.warning(f"[GH-Results] Erro ao parsear linha de resultado em {url_completa}. Pulando linha.", exc_info=True)
                    continue

            dados_da_corrida['participantes_resultado'] = lista_resultados
            # Mescla 'href_gh' com os dados raspados
            dados_completos = {**trabalho, **dados_da_corrida}
        
            # **NOTA DE PERFORMANCE:**
            # O scraper de 'card' tem uma "Passagem 2" para vídeos.
            # Isso é lento e complexo. Nós o omitimos intencionalmente
            # para focar na coleta rápida de dados de ML (pos, sp, time).
            
            return dados_completos
        
        except (TimeoutException, NoSuchElementException) as e_transiente:
                # Lógica de retry copiada do gh_scraper.py
                logging.warning(f"[GH-Results] Tentativa {attempt + 1}/{max_retries} falhou para {url_completa}. Erro: {type(e_transiente).__name__}")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                else:
                    logging.error(f"[GH-Results] Falha em {url_completa} após {max_retries} tentativas.")
                    return None
            
        except (AttributeError, ...) as e_parse:
            logging.error(f"[GH-Results] Erro de PARSING em {url_completa}. Não adianta tentar de novo.", exc_info=True)
            return None
            
    logging.error(f"[GH-Results] Falha ao raspar {url_completa} após {max_retries} tentativas. Retornando None.")
    return None