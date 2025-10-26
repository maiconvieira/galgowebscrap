import logging
import re
import time
import random
from datetime import timedelta
from bs4 import BeautifulSoup

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, WebDriverException

# Importa as funções auxiliares e configurações
from app.core import config
from app.utils.helpers import padronizar_data, analisar_posicao_final

def extrair_links_gh(driver, data_para_buscar):
    data_url = data_para_buscar
    #data_url = data_url + timedelta(days=1)
    data_url = data_url.strftime('%Y-%m-%d')
    url_diaria = f"{config.URL_BASE_GH}#meeting-list/view=time&r_date={data_url}"
    logging.info(f"Acessando Greyhound Bet para a data: {data_url}")
    
    try:
        driver.get(url_diaria)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.appList.raceList.raceListTime")))
    except TimeoutException:
        logging.error("A página do Greyhound Bet não carregou a lista de corridas a tempo.")
        return []

    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    ul_principal = soup.find('ul', class_='appList raceList raceListTime')
    if not ul_principal:
        logging.warning("A estrutura principal da página do Greyhound Bet não foi encontrada.")
        return []

    unidades_de_trabalho = []
    
    li_tags = ul_principal.find_all('li')
    for li in li_tags:
            link_tag = li.find('a')
            if not link_tag or not link_tag.has_attr('href'):
                continue

            h4 = li.find('h4')
            if h4 and h4.text.strip() in config.PISTAS_EXCLUIDAS_GH:
                logging.info(f"Pista '{h4.text.strip()}' na lista de exclusão, ignorando.")
                continue
            
            href = link_tag['href']
            if href.endswith('&tab=form'):
                href = href.replace('&tab=form', '&tab=card')

            unidades_de_trabalho.append({'href_gh': href})
    
    logging.info(f"-> {len(unidades_de_trabalho)} links de corrida encontrados no Greyhound Bet para verificação.")
    return unidades_de_trabalho

def raspar_detalhes_pagina_gh(driver, trabalho: dict, mapa_json, max_retries: int = 3):
    url_completa = f"{config.URL_BASE_GH}{trabalho['href_gh']}"
    logging.info(f"Raspando detalhes de: {url_completa}") 

    try:
        driver.get(url_completa)
        wait = WebDriverWait(driver, 15)
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.cardTabContainer")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        dados_da_corrida = {}

        h3_tag = soup.select_one('h3#pagerCardTime')
        if h3_tag: dados_da_corrida['horario'] = h3_tag.get_text(strip=True)

        h2_tag = soup.select_one('div.pageHeader h2')
        if h2_tag: dados_da_corrida['pista'] = h2_tag.get_text(strip=True).title()

        span_coluna1 = soup.select_one('span.titleColumn1')
        if span_coluna1:
            texto_coluna1 = span_coluna1.get_text(strip=True)
            match = re.search(r"Race (\d+)", texto_coluna1)
            if match: dados_da_corrida['corrida'] = match.group(1).strip()

        span_coluna2 = soup.select_one('span.titleColumn2')
        if span_coluna2:
            texto_coluna2 = span_coluna2.get_text(strip=True)
            match = re.search(r"(.+?)\s*-\s*(\d+)m\s*(.+)", texto_coluna2)
            if match:
                dados_da_corrida['categoria'] = match.group(1).strip()
                dados_da_corrida['distancia'] = int(match.group(2))
                dados_da_corrida['tipo_corrida'] = match.group(3).strip()

        p_post_pick = soup.select_one('p.p2')
        if p_post_pick:
            texto_post_pick = p_post_pick.get_text(strip=True)
            if "POST PICK:" in texto_post_pick:
                partes = texto_post_pick.split(':')
                if len(partes) > 1:
                    valor_bruto = partes[1].strip()
                    valor_final = valor_bruto.split('(')[0].strip()
                    dados_da_corrida['post_pick'] = valor_final

        # --- Extração dos Dados dos Participantes (Galgo por Galgo) ---

        lista_participantes_temp = []
        card_container_el = soup.find('div', class_='cardTabContainer')

        if card_container_el:
            faixas_corrida = card_container_el.find_all('div', class_='runnerBlock')
            for faixa_corrida in faixas_corrida:
                try:
                    faixa_num = None
                    i_tag = faixa_corrida.select_one('i[class*="trap"]')
                    if i_tag:
                        for class_name in i_tag['class']:
                            match = re.search(r'trap(\d+)', class_name)
                            if match:
                                faixa_num = int(match.group(1))
                                break

                    if faixa_num is None: continue

                    dados_participante = {'faixa': faixa_num}

                    strong_tag = faixa_corrida.select_one('a.gh strong')
                    if strong_tag: dados_participante['nome_galgo'] = strong_tag.get_text(strip=True).title()

                    comment_tag = faixa_corrida.select_one('p.comment')
                    if comment_tag: dados_participante['comment'] = comment_tag.get_text(strip=True)

                    em_form = faixa_corrida.find('em', string=re.compile(r'Form:'))
                    if em_form:
                        td_form = em_form.find_parent('td')
                        if td_form: dados_participante['form'] = td_form.get_text(strip=True).replace('Form:', '').strip()

                    em_treinador = faixa_corrida.find('em', string=re.compile(r'Tnr:'))
                    if em_treinador:
                        td_treinador = em_treinador.find_parent('td')
                        if td_treinador: dados_participante['treinador'] = td_treinador.get_text(strip=True).replace('Tnr:', '').strip().title()

                    em_sp = faixa_corrida.find('em', string=re.compile(r'SP Forecast:'))
                    if em_sp:
                        td_sp = em_sp.find_parent('td')
                        if td_sp: dados_participante['sp_forecast'] = td_sp.get_text(strip=True).replace('SP Forecast:', '').strip()

                    em_topspeed = faixa_corrida.find('em', string=re.compile(r'Topspeed:'))
                    if em_topspeed:
                        td_topspeed = em_topspeed.find_parent('td')
                        if td_topspeed: dados_participante['topspeed'] = td_topspeed.get_text(strip=True).replace('Topspeed:', '').strip()

                    lista_participantes_temp.append(dados_participante)

                except Exception as e:
                    logging.error(f"Erro ao processar participante na aba 'Card' em {url_completa}. Pulando.", exc_info=True)
                    continue

            mapa_participantes = {p['faixa']: p for p in lista_participantes_temp}

            form_tab_button = wait.until(EC.element_to_be_clickable((By.ID, "cardTab-form")))
            driver.execute_script("arguments[0].click();", form_tab_button)
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.formTabContainer")))
            time.sleep(1)

            soup_form = BeautifulSoup(driver.page_source, 'html.parser')

            lista_participantes_final = []
            form_container_el = soup_form.find('div', class_='formTabContainer')
            if form_container_el:
                faixas_corrida_form = form_container_el.find_all('div', class_='runnerBlock')
                for faixa_form in faixas_corrida_form:
                    try:
                        faixa_num_form = None
                        i_tag = faixa_form.select_one('i[class*="trap"]')
                        if i_tag:
                            for class_name in i_tag['class']:
                                match = re.search(r'trap(\d+)', class_name)
                                if match:
                                    faixa_num_form = int(match.group(1))
                                    break
                    
                        if not faixa_num_form: continue

                        dados_participante = mapa_participantes.get(faixa_num_form)
                        if not dados_participante: continue

                        dog_details_el = faixa_form.find('tr', class_='dogDetails')
                        if dog_details_el:
                            texto_detalhes = dog_details_el.get_text(strip=True)
                            padrao_detalhes = r"(\w+)\s(\w)\s(.+?)\s*-\s*(.+?)\s(\w+\d+)"
                            match = re.search(padrao_detalhes, texto_detalhes)
                            
                            if match:
                                dados_participante['cor'] = match.group(1).strip()
                                dados_participante['sexo'] = match.group(2).strip()
                                dados_participante['sire'] = match.group(3).strip().title()
                                dados_participante['dam'] = match.group(4).strip().title()
                                dados_participante['dt_nasc'] = padronizar_data(match.group(5).strip())

                        brt_el = faixa_form.find('td', class_='brt')
                        if brt_el:
                            texto_brt = brt_el.get_text(strip=True).replace('BRT:', '').strip()
                            padrao_brt = r"([\d.]+)\s+([A-Z0-9]+)\s+\((.*?)\)"
                            match = re.search(padrao_brt, texto_brt)

                            if match:
                                try:
                                    dados_participante['brt'] = float(match.group(1).strip())
                                except (ValueError, TypeError):
                                    dados_participante['brt'] = None
                                    
                                dados_participante['categoria_brt'] = match.group(2).strip()
                                dados_participante['data_brt'] = padronizar_data(match.group(3).strip())

                        # --- Extração do Histórico deste Galgo ---

                        historico_deste_galgo = []
                        tabela_historico = faixa_form.find('table', class_='formGrid')

                        if tabela_historico:
                            for linha in tabela_historico.find_all('tr')[1:]:
                                celulas = linha.find_all('td')
                                if len(celulas) == 16:

                                    try:
                                        dados_linha_hist = {
                                            'data': padronizar_data(celulas[0].get_text(strip=True)),
                                            'pista': mapa_json.get(celulas[1].get_text(strip=True), celulas[1].get_text(strip=True)),
                                            'faixa': int(celulas[3].get_text(strip=True).strip('[]')),
                                            'bends': celulas[5].get_text(strip=True),
                                            'fin': analisar_posicao_final(celulas[6].get_text(strip=True)),
                                            'by': celulas[7].get_text(strip=True),
                                            'win_sec': celulas[8].get_text(strip=True).title(),
                                            'remarks': celulas[9].get_text(strip=True),
                                            'gng': celulas[11].get_text(strip=True),
                                            'sp': celulas[13].get_text(strip=True),
                                            'grade': celulas[14].get_text(strip=True),
                                        }
                                        try: dados_linha_hist['distancia'] = int(celulas[2].get_text(strip=True).replace('m', ''))
                                        except (ValueError, TypeError): dados_linha_hist['distancia'] = None
                                        try: dados_linha_hist['split'] = float(celulas[4].get_text(strip=True))
                                        except (ValueError, TypeError): dados_linha_hist['split'] = None
                                        try: dados_linha_hist['wntm'] = float(celulas[10].get_text(strip=True))
                                        except (ValueError, TypeError): dados_linha_hist['wntm'] = None
                                        try: dados_linha_hist['wght'] = float(celulas[12].get_text(strip=True))
                                        except (ValueError, TypeError): dados_linha_hist['wght'] = None
                                        try: dados_linha_hist['caltm'] = float(celulas[15].get_text(strip=True))
                                        except (ValueError, TypeError): dados_linha_hist['caltm'] = None
                                        
                                        #video_src = None
                                        href_inicial_video = None
                                        celula_data = celulas[0]
                                        if 'videoPicture' in celula_data.get('class', []):
                                            link_tag = celula_data.find('a', class_='videoLink')
                                            if link_tag and link_tag.get('href'):
                                                href_inicial_video = link_tag.get('href')
                                                #video_src = resolver_um_video(driver, href_inicial_video)
                                        
                                        dados_linha_hist['video_href_raw'] = href_inicial_video
                                        dados_linha_hist['video_src'] = None

                                        if href_inicial_video:
                                            dados_linha_hist['video_status'] = 'pending'
                                        else:
                                            dados_linha_hist['video_status'] = 'not_found'

                                        historico_deste_galgo.append(dados_linha_hist)

                                    except Exception:
                                        logging.warning(f"Erro ao processar linha de histórico para faixa {faixa_num_form} em {url_completa}. Linha ignorada.", exc_info=True)
                                        continue

                        dados_participante['historico'] = historico_deste_galgo
                        lista_participantes_final.append(dados_participante)
                        
                    except Exception as e_part:
                        logging.error(f"Erro ao processar participante na aba 'Form' em {url_completa}. Pulando.", exc_info=True)
                        continue

        dados_da_corrida['participantes'] = lista_participantes_final
        dados_completos = {**trabalho, **dados_da_corrida}
    
        return dados_completos
        
    except (AttributeError, TypeError, ValueError, IndexError, KeyError) as e_parse:
        logging.error(f"Erro de PARSING em {url_completa}. A URL será pulada.", exc_info=True)
        return None

    return None