import re
import time
import json
import random
import logging
from bs4 import BeautifulSoup

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, WebDriverException

from app.core import config
from app.core.helpers import analisar_posicao_final, _converter_fracao_para_float, padronizar_data

def extrair_links_gh(driver, data_para_buscar):
    data_url = data_para_buscar.strftime('%Y-%m-%d')
    url_resultados = f"{config.URL_BASE_GH}#results-list/r_date={data_url}"
    logging.debug(f"Acessando Greyhound para a data de resultados: {data_url}")
    
    try:
        driver.get(url_resultados)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.results-race-list-row")))
    except TimeoutException:
        logging.error(f"[GH-Results] A página de resultados {url_resultados} não carregou a lista a tempo.")
        return []

    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    links_de_corrida = soup.select('div.results-race-list-row a')
    if not links_de_corrida:
        logging.warning("Nenhum link <a> encontrado usando o seletor 'div.results-race-list-row a'.")
        return []

    unidades_de_trabalho = []

    for link_tag in links_de_corrida:
        href = link_tag['href'] 
        unidades_de_trabalho.append({'href_gh': href})
    
    logging.debug(f"-> [GH-Results] {len(unidades_de_trabalho)} links de corrida encontrados.")
    return unidades_de_trabalho

def raspar_detalhes_pagina_gh(driver, trabalho: dict, mapa_json, max_retries: int = 3):
    url_completa = f"{config.URL_BASE_GH}{trabalho['href_gh']}"

    for attempt in range(max_retries):
        try:
            driver.get(url_completa)
            wait = WebDriverWait(driver, 30)
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.result-meeting-result")))
            time.sleep(random.uniform(0.5, 1.5))
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            dados_da_corrida = {}
            lista_participantes_resultado = []

            result_container = soup.find('div', class_='result-meeting-result')
            if not result_container:
                logging.error(f"[GH-Results] Contêiner 'result-meeting-result' não encontrado em {url_completa}.")
                return None

            h2_el = result_container.find('h2', id='headerTitle')
            if h2_el:
                partes = h2_el.get_text(strip=True).rsplit(' ', 1)
                dados_da_corrida['pista'] = partes[0].title()

            horario_el = result_container.find('h3', id='pagerResultTime')
            if horario_el:
                dados_da_corrida['horario'] = horario_el.get_text(strip=True)

            campos_para_inicializar = [
                'corrida', 'valor', 'categoria', 'distancia', 'going_gh', 
                'forecast', 'tricast', 'total_sp_percent', 'nr'
            ]

            for campo in campos_para_inicializar:
                dados_da_corrida[campo] = None
            
            race_title_el = result_container.find('div', class_='raceTitle')
            if race_title_el:
                texto_titulo = race_title_el.get_text(strip=True)
                
                match_race = re.search(r"Race\s*(\d+)", texto_titulo)
                match_prize = re.search(r"([£€])([\d\.,]+)", texto_titulo)
                match_cat = re.search(r"\((.*?)\)", texto_titulo)
                match_dist = re.search(r"(\d+)m", texto_titulo)
                match_going = re.search(r"Going:\s*([+\-\d\.]+|N)", texto_titulo, re.I)
                
                if match_race: dados_da_corrida['corrida'] = int(match_race.group(1))
                if match_prize: dados_da_corrida['valor'] = f"{match_prize.group(1)}{match_prize.group(2)}"
                if match_cat: dados_da_corrida['categoria'] = match_cat.group(1)
                if match_dist: dados_da_corrida['distancia'] = int(match_dist.group(1))
                if match_going: dados_da_corrida['going_gh'] = match_going.group(1)

            comments_container = result_container.find('div', class_='commentsContainer')
            if comments_container:
                fc_el = comments_container.find('div', string=re.compile(r'F/C:'))
                tc_el = comments_container.find('div', string=re.compile(r'T/C:'))
                sp_el = comments_container.find('div', class_='col-sp')

                if fc_el:
                    dados_da_corrida['forecast'] = fc_el.get_text(strip=True).replace('F/C:', '').strip()

                if tc_el:
                    dados_da_corrida['tricast'] = tc_el.get_text(strip=True).replace('T/C:', '').strip()

                if sp_el:
                    dados_da_corrida['total_sp_percent'] = sp_el.get_text(strip=True).replace('Total SP%:', '').strip()

                nrs_els = comments_container.find_all('p', class_='comment')
                
                if nrs_els:
                    lista_nrs = []
                    for nr in nrs_els:
                        texto_bruto = nr.get_text(strip=True)
                        texto_limpo = texto_bruto.replace('NR:', '').strip()
                        lista_nrs.append(texto_limpo)

                    dados_da_corrida['nr'] = lista_nrs

            results_list_container = result_container.find('div', class_='meetingResultsList')
            if not results_list_container:
                logging.warning(f"[GH-Results] Lista de participantes 'meetingResultsList' não encontrada em {url_completa}.")
            
            else:
                for container in results_list_container.find_all('div', class_='container'):
                    try:
                        dados_participante = {}

                        pos_el = container.find('div', class_='place')
                        pos_txt = pos_el.get_text(strip=True) if pos_el else None
                        dados_participante['posicao_final'] = analisar_posicao_final(pos_txt)

                        trap_el = container.find('div', class_=re.compile(r'bigTrap trap\d+'))
                        if trap_el:
                            match = re.search(r'trap(\d+)', ' '.join(trap_el.get('class', [])))
                            if match: dados_participante['faixa'] = int(match.group(1))

                        if not dados_participante.get('faixa'):
                            logging.warning(f"[GH-Results] Não foi possível encontrar a faixa em {url_completa}. Pulando participante.")
                            continue

                        nome_el = container.find('div', class_='name')
                        if nome_el:
                            texto_bruto = nome_el.get_text(strip=True)
                            # Regex: Remove qualquer coisa entre parenteses no inicio da string
                            # Ex: "(R2)Tysons Choice" vira "Tysons Choice"
                            texto_limpo = re.sub(r'^\(.*\)\s*', '', texto_bruto)
                            dados_participante['galgo'] = texto_limpo.title()
                        else:
                            dados_participante['galgo'] = None

                        link_el = container.find('a', class_='details')
                        dados_participante['galgo_url'] = link_el.get('href') if link_el else None

                        details_el = container.find('div', class_='dog-result-details')
                    
                        if details_el:
                            cor_el = details_el.find('span', class_='dog-color')
                            dados_participante['cor'] = cor_el.get_text(strip=True) if cor_el else None

                            sex_el = details_el.find('span', class_='dog-sex')
                            dados_participante['sex'] = sex_el.get_text(strip=True) if sex_el else None

                            nasc_el = details_el.find('span', class_='dog-date-of-birth')
                            nasc_txt = nasc_el.get_text(strip=True).replace(' ', '') if nasc_el else None
                            dados_participante['dt_nasc'] = padronizar_data(nasc_txt)

                            sire_dam_el = details_el.find('span', class_='dog-sire-dam')
                            sire_dam_txt = sire_dam_el.get_text(strip=True) if sire_dam_el else ""
                            sire, dam = None, None
                            if '-' in sire_dam_txt:
                                parts = sire_dam_txt.split('-', 1)
                                sire = parts[0].strip().title()
                                dam = parts[1].strip().title()
                            dados_participante['sire'] = sire
                            dados_participante['dam'] = dam
                        
                        else:
                            dados_participante['cor'] = None
                            dados_participante['sex'] = None
                            dados_participante['dt_nasc'] = None
                            dados_participante['sire'] = None
                            dados_participante['dam'] = None

                        col1_el = container.find('div', class_='col1')
                        col1_txt = col1_el.get_text(strip=True) if col1_el else None
                        
                        dados_participante['tempo_final_real'] = None
                        dados_participante['btn_gh'] = None

                        if col1_txt:
                            if dados_participante.get('posicao_final') == 1:
                                try:
                                    dados_participante['tempo_final_real'] = float(col1_txt)
                                except (ValueError, TypeError):
                                    pass
                            else:
                                dados_participante['btn_gh'] = col1_txt

                        sp_el = container.find('div', class_='col2')
                        sp_text = sp_el.get_text(strip=True) if sp_el else ""

                        dados_participante['sp_real'] = None
                        dados_participante['isp_info'] = None

                        if sp_text:
                            texto_upper = sp_text.upper()

                            if 'EVS' in texto_upper:
                                dados_participante['sp_real'] = 1.0
                                info_restante = texto_upper.replace('EVS', '').strip()
                                if info_restante:
                                    dados_participante['isp_info'] = info_restante

                            else:
                                fraction_part = ""
                                info_part = ""

                                for char in sp_text:
                                    if char.isdigit() or char == '/':
                                        fraction_part += char
                                    else:
                                        info_part += char

                                if fraction_part:
                                    dados_participante['sp_real'] = _converter_fracao_para_float(fraction_part)
                                
                                if info_part:
                                    dados_participante['isp_info'] = info_part.strip()

                        col3_el = container.find('div', class_='col3')
                        col3_txt = col3_el.get_text(strip=True) if col3_el else ""
                        treinador_limpo = re.sub(r'^T:\s*', '', col3_txt, flags=re.IGNORECASE)
                        dados_participante['treinador'] = treinador_limpo.strip().title()

                        comment_el = container.find('p', class_='comment')
                        dados_participante['sectional_time'] = None
                        dados_participante['remarks_gh'] = None 

                        if comment_el:
                            comment_full_text = comment_el.get_text(strip=True)

                            match_sec = re.search(r'^\(([\d\.]+)\)', comment_full_text)
                            if match_sec:
                                try:
                                    dados_participante['sectional_time'] = float(match_sec.group(1))
                                    remaining_comment = comment_full_text[len(match_sec.group(0)):].strip()
                                    if remaining_comment:
                                        dados_participante['remarks_gh'] = remaining_comment
                                except ValueError:
                                    dados_participante['remarks_gh'] = comment_full_text
                            else:
                                dados_participante['remarks_gh'] = comment_full_text

                        lista_participantes_resultado.append(dados_participante)

                    except Exception as e_parse:
                        logging.warning(f"[GH-Results] Erro ao parsear um participante em {url_completa}. Pulando.", exc_info=True)
                        continue

            dados_da_corrida['participantes_resultado'] = lista_participantes_resultado
            dados_completos = {**trabalho, **dados_da_corrida}
            
            return dados_completos
        
        except (TimeoutException, NoSuchElementException) as e_transiente:
                #driver.save_screenshot(f'debug_falha_gh_{attempt + 1}.png')
                logging.warning(f"[GH-Results] Tentativa {attempt + 1}/{max_retries} falhou para {url_completa}. Erro: {type(e_transiente).__name__}")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                else:
                    logging.error(f"[GH-Results] Falha em {url_completa} após {max_retries} tentativas.")
                    return None
            
        except AttributeError as e_parse:
            logging.error(f"[GH-Results] Erro de PARSING (AttributeError) em {url_completa}. Não adianta tentar de novo.", exc_info=True)
            return None
        except Exception as e_geral:
            logging.critical(f"[GH-Results] Erro fatal inesperado em {url_completa}", exc_info=True)
            return None
            
    logging.error(f"[GH-Results] Falha ao raspar {url_completa} após {max_retries} tentativas. Retornando None.")
    return None