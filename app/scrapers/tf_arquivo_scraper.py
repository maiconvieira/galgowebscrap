import re
import time
import json
import random
import logging
from bs4 import BeautifulSoup

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

from app.core import config
from app.core.helpers import analisar_posicao_final, _converter_fracao_para_float, _limpar_rating_para_int

def extrair_links_tf(driver, data_para_buscar):
    data_str = data_para_buscar.strftime('%Y-%m-%d')
    url_resultados = f"{config.URL_BASE_TF}/greyhound-racing/results/{data_str}"
    logging.info(f"Acessando Timeform para a data de resultados: {data_str}")

    try:
        driver.get(url_resultados)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "waf-meeting-races")))
        time.sleep(random.uniform(1, 3))
    except TimeoutException:
        soup_check = BeautifulSoup(driver.page_source, 'html.parser')
        if soup_check.find(text=re.compile(r"no results available|no meetings found", re.I)):
            logging.warning(f"Nenhuma corrida encontrada para {data_str} (dia vazio).")
            return []
        logging.error(f"Timeout ao carregar a página de lista de resultados para {data_str}", exc_info=True)
        return []

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    section_principal = soup.find('section', class_='w-archive-full')
    if not section_principal:
        logging.warning("A estrutura principal da página de resultados Timeform não foi encontrada.")
        return []

    unidades_de_trabalho = []

    for corrida_link in section_principal.find_all('a', class_='waf-header'):
        href = corrida_link.get('href')
        if href:
            unidades_de_trabalho.append({
                'href_tf': href
            })
    
    logging.info(f"-> {len(unidades_de_trabalho)} links de resultados encontrados no Timeform.")
    return unidades_de_trabalho

def raspar_detalhes_pagina_tf(driver, trabalho: dict, mapa_json, max_retries: int = 3):
    url_completa = f"{config.URL_BASE_TF}{trabalho['href_tf']}"
    logging.info(f"Raspando resultados de: {url_completa}")

    for attempt in range(max_retries):
        try:
            driver.get(url_completa)
            wait = WebDriverWait(driver, 20)

            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "section.mb-bfw-result")))
            except TimeoutException:
                logging.info(f"Nenhuma tabela de histórico encontrada em {url_completa} após a espera. Provavelmente não há dados de histórico.")

            time.sleep(random.uniform(0.5, 2))

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            body = soup.find('body')
            if body:
                body_content = body.get_text(strip=True)
                block_page_text = 'For data, please visit https://www.globalsportsapi.com/'
                if block_page_text in body_content:
                    logging.warning(f"Página de bloqueio (GSA) detectada em {url_completa}. Acionando retentativa...")
                    raise TimeoutException("Página de bloqueio (GSA) detectada.")

            dados_da_corrida = {}

            container_resultados = soup.find('section', class_='mb-bfw-result mb-bfw')
            if not container_resultados:
                logging.error(f"Contêiner de resultados 'mb-bfw-result mb-bfw' não encontrado em {url_completa}.")
                return None

            h1_el = soup.find('h1', class_='w-header')
            if h1_el:
                h1_str = h1_el.get_text(strip=True).strip()
                try:
                    horario, pista = ' '.join(h1_str.split()).split(' ', 1)
                    dados_da_corrida['horario'] = horario
                    dados_da_corrida['pista'] = pista.title()
                except ValueError:
                    dados_da_corrida['h1_completo'] = h1_str

            details_el = soup.find('div', class_='rph-race-details')
            if details_el:
                col1_els = details_el.find_all('div', class_='rph-race-details-col-1')
                if len(col1_els) > 1:
                    texto1_col1 = col1_els[1].get_text(strip=True)
                    match_g = re.search(r"Grade:\((.*?)\)", texto1_col1)
                    match_d = re.search(r"Distance:(\d+)m", texto1_col1)
                    match_t = re.search(r"Racing:(\w+)", texto1_col1)
                    dados_da_corrida['categoria'] = match_g.group(1) if match_g else None
                    dados_da_corrida['distancia'] = int(match_d.group(1)) if match_d else None
                    dados_da_corrida['tipo_corrida'] = match_t.group(1) if match_t else None
                if len(col1_els) > 2:
                    texto2_col1 = col1_els[2].get_text(strip=True)
                    match_tf = re.search(r"Tf\sGoing:\s*([-\d\.]+)", texto2_col1)
                    match_going = re.search(r"Going:\s*([-\d\.]+)$", texto2_col1)
                    dados_da_corrida['going_tf'] = float(match_tf.group(1)) if match_tf else None
                    dados_da_corrida['going'] = float(match_going.group(1)) if match_going else None

                col2_els = details_el.find_all('div', class_='rph-race-details-col-2')
                if len(col2_els) > 1:
                    texto1_col2 = col2_els[1].get_text(strip=True)
                    texto_corrigido = texto1_col2.replace("Prizes:", "Prizes: ").replace("Total:", " Total: ")
                    dados_da_corrida['premios'] = ' '.join(texto_corrigido.split())
                if len(col2_els) > 2:
                    texto2_col2 = col2_els[2].get_text(strip=True)
                    match_fc = re.search(r"Forecast:\s*£?([\d\.]+)", texto2_col2)
                    match_tc = re.search(r"Tricast:\s*£?([\d\.]+)$", texto2_col2)
                    dados_da_corrida['forecast'] = float(match_fc.group(1)) if match_fc else None
                    dados_da_corrida['tricast'] = float(match_tc.group(1)) if match_tc else None

            cartao_el = soup.find('section', class_='w-seo-content')
            if cartao_el:
                dados_da_corrida['cartao_corrida'] = ' '.join(cartao_el.get_text(separator=' ', strip=True).split())

            # --- Extração dos Dados dos Participantes (Galgo por Galgo) ---

            lista_participantes_resultado = []

            tabela_resultados_body = container_resultados.find('tbody', class_='rrb')

            if tabela_resultados_body:
                for linha_1 in tabela_resultados_body.find_all('tr', class_='rrb-runner-details-1'):
                    linha_2 = linha_1.find_next_sibling('tr', class_='rrb-runner-details-2')
                    
                    if not linha_2:
                        logging.warning(f"Não foi possível encontrar a linha 2 (details-2) para um participante em {url_completa}")
                        continue
                    
                    try:
                        dados_participante = {}
                        pos_el = linha_1.find('td', class_='rrb-pos')
                        pos_txt = pos_el.get_text(strip=True) if pos_el else None
                        dados_participante['posicao_final'] = analisar_posicao_final(pos_txt)

                        btn_el = linha_1.find('td', title=lambda t: t and 'behind the greyhound that finished' in t)
                        dados_participante['btn'] = btn_el.get_text(strip=True) if btn_el else None

                        faixa_el = linha_1.find('img', class_='rrb-trap')
                        faixa_str = faixa_el.get('alt', '0') if faixa_el else '0'
                        dados_participante['faixa'] = int(faixa_str)

                        galgo_el = linha_1.find('a', class_='rrb-greyhound')
                        dados_participante['galgo'] = galgo_el.get_text(strip=True).title() if galgo_el else None
                        dados_participante['galgo_url'] = galgo_el.get('href') if galgo_el else None

                        age_sex_el = linha_1.find('td', title="The age and sex of the greyhound")
                        dados_participante['age'] = None
                        dados_participante['sex'] = None

                        if age_sex_el:
                            age_sex_txt = age_sex_el.get_text(strip=True)
                            if age_sex_txt and len(age_sex_txt) > 1:
                                age_str = age_sex_txt[:-1]
                                sex_str = age_sex_txt[-1]
                                try:
                                    dados_participante['age'] = int(age_str)
                                    dados_participante['sex'] = sex_str
                                except ValueError:
                                    logging.warning(f"Não foi possível parsear idade/sexo de '{age_sex_txt}' na URL {url_completa}")
                            elif age_sex_txt:
                                logging.warning(f"Formato de idade/sexo inesperado: '{age_sex_txt}' na URL {url_completa}")

                        bend_el = linha_1.find('span', title=lambda t: t and 'position of the greyhound at the bends' in t)
                        dados_participante['bend'] = bend_el.get_text(strip=True) if bend_el else None

                        comments_el = linha_1.find('span', title=lambda t: t and 'run comment in this race' in t)
                        dados_participante['comment'] = comments_el.get_text(strip=True) if comments_el else None

                        isp_el = linha_1.find('span', title="The official starting price of the greyhound in this race")
                        dados_participante['sp_real'] = None
                        dados_participante['isp_info'] = None

                        if isp_el:
                            isp_text = isp_el.get_text(strip=True)
                            if not isp_text:
                                pass
                            elif isp_text.lower() == 'evs':
                                dados_participante['sp_real'] = 1.0
                            else:
                                fraction_part = ""
                                info_part = ""
                                
                                for char in isp_text:
                                    if char.isdigit() or char == '/':
                                        fraction_part += char
                                    else:
                                        info_part += char
                                
                                if fraction_part:
                                    dados_participante['sp_real'] = _converter_fracao_para_float(fraction_part)
                                
                                if info_part:
                                    dados_participante['isp_info'] = info_part.strip()

                        tfr_el = linha_1.find('span', title=lambda t: t and 'rating based on the greyhound' in t)
                        tfr_str = tfr_el.get_text(strip=True) if tfr_el else None
                        dados_participante['tfr'] = _limpar_rating_para_int(tfr_str)

                        tempo_el = linha_2.find('span', title=lambda t: t and 'official run time' in t)
                        dados_participante['tempo_final_real'] = None
                        dados_participante['sectional_time'] = None

                        if tempo_el:
                            tempo_txt_completo = tempo_el.get_text(strip=True)
                            match_full = re.search(r'([\d\.]+) \(([\d\.]+)\)', tempo_txt_completo)
                            match_simple = re.search(r'^([\d\.]+)', tempo_txt_completo)

                            if match_full:
                                try:
                                    dados_participante['tempo_final_real'] = float(match_full.group(1))
                                    dados_participante['sectional_time'] = float(match_full.group(2))
                                except (ValueError, IndexError):
                                    logging.warning(f"Não foi possível parsear tempo/sectional de '{tempo_txt_completo}' na URL {url_completa}")
                            
                            elif match_simple:
                                try:
                                    dados_participante['tempo_final_real'] = float(match_simple.group(1))
                                except ValueError:
                                    logging.warning(f"Não foi possível parsear tempo de '{tempo_txt_completo}' na URL {url_completa}")
                            
                            else:
                                logging.info(f"Nenhum tempo numérico encontrado em '{tempo_txt_completo}' na URL {url_completa}")

                        trainer_el = linha_2.find('span', title="The full name of the greyhound's trainer")
                        dados_participante['trainer'] = trainer_el.get_text(strip=True).title() if trainer_el else None

                        bsp_el = linha_2.find('span', title="The Betfair starting price of the greyhound in this race")
                        dados_participante['bsp'] = None

                        if bsp_el:
                            bsp_text = bsp_el.get_text(strip=True)
                            if bsp_text:
                                try:
                                    dados_participante['bsp'] = float(bsp_text)
                                except ValueError:
                                    logging.warning(f"Não foi possível converter BSP '{bsp_text}' para float na URL {url_completa}")

                        if dados_participante['faixa'] > 0:
                            lista_participantes_resultado.append(dados_participante)

                    except Exception as e_parse:
                        logging.error(f"Erro ao parsear um participante em {url_completa}.", exc_info=True)
                        continue
            
            dados_da_corrida['participantes_resultado'] = lista_participantes_resultado
            dados_completos = {**trabalho, **dados_da_corrida}
            
            return dados_completos

        except (TimeoutException, NoSuchElementException) as e:
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
        except Exception as e_geral:
            logging.critical(f"[TF-Results] Erro fatal inesperado em {url_completa}", exc_info=True)
            return None
    
    logging.error(f"[TF-Results] Falha ao raspar {url_completa} após {max_retries} tentativas. Retornando None.")
    return None