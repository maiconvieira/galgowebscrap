import logging
import re
import time
from bs4 import BeautifulSoup

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Importa as funções auxiliares e configurações
from app.core import config
from app.utils.helpers import padronizar_data, analisar_posicao_final

def extrair_links_tf(driver, data_para_buscar):
    data_url = data_para_buscar.strftime('%Y-%m-%d')
    url_diaria = f"{config.URL_BASE_TF}/greyhound-racing/racecards"
    
    logging.info(f"Acessando Timeform para a data: {data_url}")
    
    try:
        driver.get(url_diaria)
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "wfr-bytrack-content")))
    except TimeoutException:
        logging.error("A página do Timeform não carregou a lista de corridas a tempo.")
        return []

    sort_time_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-setting='GreyhoundsRacecardSort']")))
    driver.execute_script("arguments[0].click();", sort_time_button)
    time.sleep(1)

    ##############################################################################
    #try:
        #botao_amanha = driver.find_element(By.XPATH, "//button[text()=\"Tomorrow's Racing\"]")
        #botao_amanha.click()
        #time.sleep(5) 
    #except Exception as e:
        #print(f"!! Aviso: Não foi possível clicar no botão de amanhã. Erro: {e}")
    ##############################################################################

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    div_principal = soup.find('div', class_='wfr-bytrack-content wfr-content')
    if not div_principal:
        logging.warning("A estrutura principal da página do Timeform não foi encontrada.")
        return []

    unidades_de_trabalho = []
    
    for dados_pista in div_principal.find_all('li'):
        for corrida_link in dados_pista.find_all('a'):
            href = corrida_link.get('href')
            if href:
                unidades_de_trabalho.append({
                    'href_tf': href
                })
    
    logging.info(f"-> {len(unidades_de_trabalho)} links de corrida encontrados no Timeform para verificação.")
    return unidades_de_trabalho

def raspar_detalhes_pagina_tf(driver, trabalho: dict, mapa_json, max_retries: int = 3):
    url_completa = f"{config.URL_BASE_TF}{trabalho['href_tf']}"
    logging.info(f"Raspando detalhes de: {url_completa}")

    for attempt in range(max_retries):
        try:
            driver.get(url_completa)
            wait = WebDriverWait(driver, 20)

            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.recent-form tbody tr")))
            except TimeoutException:
                logging.info(f"Nenhuma tabela de histórico encontrada em {url_completa} após a espera. Provavelmente não há dados de histórico.")

            time.sleep(1)

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            body = soup.find('body')
            if body:
                body_content = body.get_text(strip=True)
                block_page_text = 'For data, please visit https://www.globalsportsapi.com/'

                if block_page_text in body_content:
                    logging.warning(f"Página de bloqueio (GSA) detectada em {url_completa}. Acionando retentativa...")
                    raise TimeoutException("Página de bloqueio (GSA) detectada.")

            dados_da_corrida = {}

            # --- Extração dos Dados Gerais da Corrida (com segurança) ---

            h1_el = soup.find('h1', class_='w-header')
            if h1_el:
                h1_str = h1_el.get_text(strip=True).replace(" Racecard", "").strip()
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
                    texto_col1 = col1_els[1].get_text(strip=True)
                    match_g = re.search(r"Grade:\((.*?)\)", texto_col1)
                    match_d = re.search(r"Distance:(\d+)m", texto_col1)
                    match_t = re.search(r"Racing:(\w+)", texto_col1)
                    dados_da_corrida['categoria'] = match_g.group(1) if match_g else None
                    dados_da_corrida['distancia'] = int(match_d.group(1)) if match_d else None
                    dados_da_corrida['tipo_corrida'] = match_t.group(1) if match_t else None

                col2_els = details_el.find_all('div', class_='rph-race-details-col-2')
                if len(col2_els) > 1:
                    texto_col2 = col2_els[1].get_text(strip=True)
                    texto_corrigido = texto_col2.replace("Prizes:", "Prizes: ").replace("Total:", " Total: ")
                    dados_da_corrida['premios'] = ' '.join(texto_corrigido.split())

                perfil_el = details_el.find('p', class_='rph-track-profile')
                if perfil_el:
                    dados_da_corrida['perfil_pista'] = perfil_el.get_text(strip=True).replace("Track Profile:", "").strip()

            favoritos_el = soup.find('p', class_='rpf-betting-forecast')
            if favoritos_el:
                texto_bruto = favoritos_el.get_text(strip=True).replace("Betting Forecast:", "").strip()
                texto_com_espacos = re.sub(r'(\d+/\d+)([A-Za-z])', r'\1 \2', texto_bruto)
                dados_da_corrida['favoritos'] = texto_com_espacos.replace(',', ', ')

            cartao_el = soup.find('section', class_='w-seo-content')
            if cartao_el:
                dados_da_corrida['cartao_corrida'] = ' '.join(cartao_el.get_text(separator=' ', strip=True).split())

            # --- Extração dos Dados dos Participantes (Galgo por Galgo) ---

            lista_participantes = []
            faixas_corrida = soup.find_all('tbody', class_='rpb')
            for faixa_corrida in faixas_corrida:

                if not faixa_corrida:
                    logging.warning(f"Item 'faixa_corrida' nulo encontrado na URL {url_completa}. Pulando.")
                    continue

                try:
                    faixa_num = int(faixa_corrida.get('data-trap', 0))
                    if faixa_num == 0: continue

                    dados_participante = {'faixa': faixa_num}

                    galgo_el = faixa_corrida.find('a', class_=f"rpb-greyhound")
                    if galgo_el: dados_participante['nome_galgo'] = galgo_el.get_text(strip=True).title()

                    winrec_el = faixa_corrida.find('span', title="Greyhound's career win record for this type of racing")
                    if winrec_el: dados_participante['win_rec'] = winrec_el.get_text(strip=True)

                    traprec_el = faixa_corrida.find('span', title="Greyhound's career win record at this trap for this type of racing")
                    if traprec_el: dados_participante['trap_rec'] = traprec_el.get_text(strip=True)

                    mstr_el = faixa_corrida.find('div', class_='rpb-rating rpb-final-rating')
                    if mstr_el: dados_participante['mstr'] = mstr_el.get_text(strip=True)

                    sect_el = faixa_corrida.find('div', class_='rpb-rating rpb-sectional-rating')
                    if sect_el: dados_participante['sect'] = sect_el.get_text(strip=True)

                    form_el = faixa_corrida.find('span', title='The previous 5 finishing positions of this greyhound')
                    if form_el: dados_participante['form'] = form_el.get_text(strip=True)

                    trainer_full_el = faixa_corrida.find('span', title=lambda t: t and 'trainer' in t)
                    if trainer_full_el:
                        trainer_full = trainer_full_el.get_text(strip=True)
                        if '(' in trainer_full:
                            partes = trainer_full.split(' (', 1)
                            dados_participante['treinador'] = partes[0].strip().title()
                            dados_participante['strike_rate'] = partes[1].replace(')', '').strip() if len(partes) > 1 else None
                        else:
                            dados_participante['treinador'] = trainer_full.strip().title()

                    details_el = faixa_corrida.find('span', title="The greyhound's gender, colour and DOB")
                    if details_el:
                        parts = details_el.get_text(strip=True).split(' ', 2)
                        dados_participante['cor'] = parts[0] if len(parts) > 0 else None
                        dados_participante['sexo'] = parts[1] if len(parts) > 1 else None
                        dt_nascimento_faixa_corrida_orig = parts[2] if len(parts) > 2 else None
                        dados_participante['dt_nasc'] = padronizar_data(dt_nascimento_faixa_corrida_orig)

                    pedigree_els = faixa_corrida.find_all('span', class_='rp-setting-pedigree')
                    if len(pedigree_els) > 0: dados_participante['sire'] = pedigree_els[0].get_text(strip=True).title()
                    if len(pedigree_els) > 1: dados_participante['dam'] = pedigree_els[1].get_text(strip=True).title()

                    seed_el = faixa_corrida.find('span', title="The greyhound's seed")
                    if seed_el: dados_participante['seed'] = seed_el.get_text(strip=True)

                    comment_el = faixa_corrida.find('b', title="Timeform's comment summing up the prospect for each greyhound in this race")
                    if comment_el: dados_participante['comment'] = comment_el.get_text(strip=True)

                    # --- Extração do Histórico deste Galgo ---

                    historico_deste_galgo = []

                    tabela_hist_el = faixa_corrida.find('table', class_='recent-form')
                    if tabela_hist_el and tabela_hist_el.find('tbody'):
                        for linha in tabela_hist_el.find('tbody').find_all('tr'):

                            if not hasattr(linha, 'get'):
                                    continue

                            if 'run-comment-mob' in linha.get('class', []): continue

                            tds = linha.find_all('td')
                            if len(tds) < 17: continue

                            try:
                                dados_linha_hist = {}

                                celula_data = tds[0]
                                dados_linha_hist['data'] = padronizar_data(celula_data.get_text(strip=True))
                                dados_linha_hist['observacoes'] = celula_data.get('title', '').strip()
                                celula_tipo = tds[1]
                                dados_linha_hist['tipo_corrida'] = celula_tipo.get('title', '').strip()
                                celula_pista = tds[2]
                                dados_linha_hist['pista'] = celula_pista.get('title', '').strip()
                                try:
                                    dados_linha_hist['distancia'] = int(tds[3].get_text(strip=True).replace('m', ''))
                                except (ValueError, TypeError):
                                    dados_linha_hist['distancia'] = None
                                dados_linha_hist['categoria'] = tds[4].get_text(strip=True)
                                dados_linha_hist['eye'] = tds[5].get_text(strip=True)
                                dados_linha_hist['proxy'] = tds[6].get_text(strip=True)
                                dados_linha_hist['faixa'] = tds[7].get_text(strip=True)
                                try:
                                    dados_linha_hist['tf_sec'] = float(tds[8].get_text(strip=True))
                                except (ValueError, TypeError):
                                    dados_linha_hist['tf_sec'] = None
                                dados_linha_hist['bend'] = tds[9].get_text(strip=True)
                                dados_linha_hist['fin'] = analisar_posicao_final(tds[10].get_text(strip=True))
                                dados_linha_hist['btn_by'] = tds[11].get_text(strip=True)
                                dados_linha_hist['tf_going'] = tds[12].get_text(strip=True)
                                dados_linha_hist['isp'] = tds[13].get_text(strip=True)
                                try:
                                    dados_linha_hist['tf_time'] = float(tds[14].get_text(strip=True))
                                except (ValueError, TypeError):
                                    dados_linha_hist['tf_time'] = None
                                dados_linha_hist['sec_rtg'] = tds[15].get_text(strip=True)
                                dados_linha_hist['rtg'] = tds[16].get_text(strip=True)

                                historico_deste_galgo.append(dados_linha_hist)

                            except (ValueError, TypeError, IndexError) as e:
                                logging.warning(f"Não foi possível parsear uma linha do histórico para o galgo na faixa {faixa_num}. Erro: {e}. Linha ignorada.")
                                continue

                    dados_participante['historico'] = historico_deste_galgo
                    lista_participantes.append(dados_participante)

                except Exception as e:
                    logging.error(f"Erro ao processar um participante na URL {url_completa}. Pulando participante.", exc_info=True)
                    continue
            
            dados_da_corrida['participantes'] = lista_participantes
            dados_completos = {**trabalho, **dados_da_corrida}
            
            return dados_completos

        except (TimeoutException, NoSuchElementException) as e:
            logging.warning(f"Tentativa {attempt + 1}/{max_retries} falhou para {url_completa}. Erro: {type(e).__name__}.")

            if attempt < max_retries - 1:
                sleep_time = (attempt + 1) * 2
                logging.info(f"Aguardando {sleep_time} segundos antes de tentar novamente.")
                time.sleep(sleep_time)
                # Tentar um refresh pode ajudar
                try:
                    driver.refresh()
                except WebDriverException as e_refresh:
                    # Se o refresh falhar, o driver provavelmente morreu.
                    logging.error(f"Falha ao dar refresh no driver. Abortando retentativas.", exc_info=True)
                    # Deixa a exceção original (e_refresh) subir para o worker
                    raise e_refresh
            else:
                logging.error(f"Todas as {max_retries} tentativas falharam para {url_completa} (erros de elemento/timeout). Descartando esta tarefa.")
                return None
    
    logging.error(f"Falha ao raspar {url_completa} após {max_retries} tentativas. Retornando None.")
    return None