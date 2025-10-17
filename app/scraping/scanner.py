import time
import re
import random
import json
from datetime import date, timedelta, datetime
from bs4 import BeautifulSoup
from sqlalchemy.orm.exc import NoResultFound

import undetected_chromedriver as uc

# IMPORTAÇÕES ESSENCIAIS PARA ESPERAS EXPLÍCITAS
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Importações do nosso projeto
from app.db.conexao import get_db
from app.db.modelos import Pista, Corrida, Galgo, Treinador, Participacao, HistoricoCorrida

def configurar_driver():
    options = uc.ChromeOptions()
    #options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, use_subprocess=True)
    return driver

def padronizar_data(texto_data):
    if not texto_data:
        return None

    formatos_possiveis = [
        '%d/%m/%Y',  # Ex: 25/12/2023
        '%d%b%y',    # Ex: 25Dec23
        '%Y-%m-%d',  # Ex: 2023-12-25
        '%b%y'       # Ex: Dec23
    ]

    for formato in formatos_possiveis:
        try:
            return datetime.strptime(texto_data, formato).date()
        except ValueError:
            continue

    print(f"!! Aviso: Não foi possível padronizar a data '{texto_data}' com os formatos conhecidos.")
    return None

def analisar_posicao_final(finish_text):
    if not finish_text:
        return None
    cleaned = finish_text.rstrip('ndrdsth').upper()

    if cleaned in ['NR', 'F', 'D', 'R', 'UR', 'PU', 'BD', 'RO']:
        return None
    
    try:
        return int(cleaned)
    except (ValueError, TypeError):
        return None

def extrair_dados_tf(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    div_principal = soup.find('div', class_='wfr-bytrack-content wfr-content')
    if not div_principal:
        return []
    
    dados = []
    for dados_pista in div_principal.find_all('div', class_='wfr-meeting'):
        pista = dados_pista.find('b', class_='wfr-track').text.strip()
        
        for corrida_link in dados_pista.find_all('a', class_='wfr-race bg-light-gray hover-opacity'):
            attr_title = corrida_link.get('title', '')
            
            if not attr_title.startswith(f"{pista} R"):
                continue

            attr_title_reduzido = attr_title.replace(f"{pista} ", "", 1).replace(" BAGS ", " ").replace(" Flat", "").strip()
            title_parts = attr_title_reduzido.split()

            if len(title_parts) < 3:
                continue

            # Extrai as partes para variáveis claras
            num_corrida_str = title_parts[0]
            categoria_str = title_parts[1]
            distancia_com_m = title_parts[2] # Ex: "480m"

            # Validação da categoria
            filtro_categoria = (
                categoria_str.startswith('A') and
                len(categoria_str) > 1 and
                categoria_str[1:].isdigit() and
                2 <= int(categoria_str[1:]) <= 9
            )

            # Validação da distância (corrigida)
            distancia_numerica_str = distancia_com_m.replace('m', '')
            filtro_distancia = (
                distancia_numerica_str.isdigit() and
                300 <= int(distancia_numerica_str) <= 600
            )

            # Adiciona os dados somente se AMBOS os filtros passarem
            if filtro_categoria and filtro_distancia:
                horario = corrida_link.text.strip()
                link = corrida_link.get('href', '')
                num_corrida_limpo = num_corrida_str.replace("R", "")
                
                # Adiciona os dados limpos e validados
                dados.append({
                    'pista': pista,
                    'horario': horario,
                    'href': link,
                    'corrida': num_corrida_limpo,
                    'categoria': categoria_str,
                    'distancia': distancia_numerica_str # <-- Salva SÓ o número
                })
                
    return dados

def extrair_dados_gh(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    ul_principal = soup.find('ul', class_='appList raceList raceListTime')
    if not ul_principal: return []

    dados = []
    pistas_excluidas = ['Youghal', 'Shelbourne Park', 'Thurles', 'Limerick', 'Cork', 'Lifford', 'Tralee', 'Mullingar', 'Waterford', 'Dundalk', 'Drumbo Park', 'Kilkenny']
    for li in ul_principal.find_all('li'):
        link = li.find('a')
        href = link.get('href', '')
        event_label = link.get('data-eventlabel', '')
        event_parts = [part.strip() for part in event_label.split(',')] if event_label else []
        pista = event_parts[0]

        if pista in pistas_excluidas:
            continue

        corrida_el = link.find('h5')
        detalhes_el = link.find('em')

        horario = event_parts[1]
        corrida_info = corrida_el.text.strip()
        detalhes = detalhes_el.text.strip()

        categoria = None
        distancia = None

        if 'Grade:' in detalhes:
            categoria_start = detalhes.find('(') + 1
            categoria_end = detalhes.find(')', categoria_start)
            if categoria_end != -1:
                categoria = detalhes[categoria_start:categoria_end].strip()
                filtro_categoria = (
                    categoria.startswith('A') and
                    len(categoria) > 1 and
                    categoria[1:].isdigit() and
                    2 <= int(categoria[1:]) <= 9
                )

                if 'Dis:' in detalhes:
                    dis_start = detalhes.find('Dis:') + 4
                    dis_end = detalhes.find('m', dis_start)
                    if dis_end != -1:
                        distancia = detalhes[dis_start:dis_end].strip()
                        filtro_distancia = (distancia.isdigit() and 300 <= int(distancia) <= 600)
                        if filtro_categoria and filtro_distancia:

                            corrida_numero = None
                            if 'Race' in corrida_info:
                                try:
                                    corrida_numero = corrida_info.split('Race')[1].strip().split()[0]
                                except:
                                    corrida_numero = corrida_info
                            else:
                                corrida_numero = corrida_info

                            dados.append({'pista': pista, 'horario': horario, 'href': href, 'corrida': corrida_numero, 'categoria': categoria or 'N/A', 'distancia': distancia or 'N/A'})

    return dados

def extrair_corridas_iniciais(driver):
    time_form_url = "https://www.timeform.com/greyhound-racing/racecards"
    greyhound_url = "https://greyhoundbet.racingpost.com/#meeting-list/view=time"
    #greyhound_url = "https://greyhoundbet.racingpost.com/#meeting-list/view=time&r_date=2025-10-17"

    driver.get(time_form_url)
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "wfr-bytrack-content")))
        #############################################################################
        #try:
            #botao_amanha = driver.find_element(By.XPATH, "//button[text()=\"Tomorrow's Racing\"]")
            #botao_amanha.click()
            #time.sleep(5) 
        #except Exception as e:
            #print(f"!! Aviso: Não foi possível clicar no botão de amanhã. Erro: {e}")
        #############################################################################
        dados_corridas_tf = extrair_dados_tf(driver.page_source)
    except TimeoutException:
        print("Erro: A página do TimeForm não carregou os elementos esperados a tempo.")
        dados_corridas_tf = []

    driver.get(greyhound_url)
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "raceListTime")))
        dados_corridas_gh = extrair_dados_gh(driver.page_source)     
    except Exception as e:
        print(f"Erro ao processar GreyhoundBet: {e}")
        dados_corridas_gh = []

    registros_mesclados = []
    for registro_tf in dados_corridas_tf:
        for registro_gh in dados_corridas_gh:
            if all([
                registro_tf['pista'] == registro_gh['pista'],
                registro_tf['horario'] == registro_gh['horario'],
                registro_tf['corrida'] == registro_gh['corrida']
            ]):
                registros_mesclados.append({
                    'pista': registro_tf['pista'],
                    'horario': registro_tf['horario'],
                    'corrida': registro_tf['corrida'],
                    'categoria': registro_tf.get('categoria', 'N/A'),
                    'distancia': registro_tf.get('distancia', 'N/A'),
                    'href_tf': registro_tf['href'],
                    'href_gh': registro_gh['href']
                })
                break 
    
    registros_mesclados_ordenados = sorted(registros_mesclados, key=lambda x: x['horario'])
    print(f"{len(registros_mesclados_ordenados)} corridas para processamento.")
    return registros_mesclados_ordenados

def processar_detalhes_corrida(driver, corrida_info, db, mapa_pistas):
    wait = WebDriverWait(driver, 15)

    historico_compilado = {}
    videos_para_resolver = []

    url_tf = f"https://www.timeform.com{corrida_info['href_tf']}"
    driver.get(url_tf)
    try:
        recent_form_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-section='recent-form']")))
        driver.execute_script("arguments[0].click();", recent_form_button)
        time.sleep(1)
    except (TimeoutException, NoSuchElementException) as e:
        print(f"!! Erro ao carregar ou interagir com a página do TimeForm: {e}")
        return False
    soup_tf = BeautifulSoup(driver.page_source, 'html.parser')

    url_gh = f"https://greyhoundbet.racingpost.com/{corrida_info['href_gh']}"
    driver.get(url_gh)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".level.level-5.card")))
        #wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".level.level-5.form")))
        time.sleep(1)
        soup_gh = BeautifulSoup(driver.page_source, 'html.parser')
    except (TimeoutException, NoSuchElementException) as e:
        print(f"!! Aviso: Não foi possível carregar a página do GreyHound. Pulando enriquecimento. Erro: {e}")
        soup_gh = None

    nome_pista_padronizado = corrida_info['pista'].title()
    pista = db.query(Pista).filter(Pista.nome == nome_pista_padronizado).first()
    if not pista:
        pista = Pista(nome=nome_pista_padronizado)
        db.add(pista)
        db.flush()

    premios_el = soup_tf.find_all('div', class_='rph-race-details-col rph-race-details-col-2')
    premios = premios_el[1].get_text(separator=' ', strip=True).replace("Prizes: ", "").strip() if len(premios_el) > 1 else None
    perfil_pista_el = soup_tf.find('p', class_='rph-track-profile')
    perfil_pista = perfil_pista_el.find('b').get_text(strip=True) if perfil_pista_el and perfil_pista_el.find('b') else None
    favoritos_tf_el = soup_tf.find('p', class_='rpf-betting-forecast')
    texto_bruto_favoritos = favoritos_tf_el.get_text(strip=True).replace("Betting Forecast:", "").strip() if favoritos_tf_el else None
    favoritos_tf = None
    if texto_bruto_favoritos:
        texto_com_espacos = re.sub(r'(\d+/\d+)([A-Za-z])', r'\1 \2', texto_bruto_favoritos)
        favoritos_tf = texto_com_espacos.replace(',', ', ')

    cartao_corrida_el = soup_tf.find('section', class_='w-seo-content')
    texto_bruto_cartao_corrida = cartao_corrida_el.get_text(separator=' ', strip=True) if cartao_corrida_el else None
    cartao_corrida = None
    if texto_bruto_cartao_corrida:
        cartao_corrida_limpo = ' '.join(texto_bruto_cartao_corrida.split())
        cartao_corrida = cartao_corrida_limpo.replace(" .", ".").strip()

    favoritos_gh = None
    if soup_gh:
        post_pick_el = soup_gh.find('p', class_='p2')
        if post_pick_el and "POST PICK:" in post_pick_el.text:
            favoritos_gh = post_pick_el.text.replace("POST PICK:", "").strip().split(' (')[0]

    nova_corrida = Corrida(
        pista_id=pista.id, horario=corrida_info['horario'], data_corrida = date.today() + timedelta(days=1),
        numero_corrida=corrida_info['corrida'], categoria=corrida_info['categoria'], distancia=int(corrida_info['distancia']),
        premios=premios, perfil_pista=perfil_pista, favoritos_tf=favoritos_tf, favoritos_gh=favoritos_gh, cartao_corrida=cartao_corrida, 
        href_tf=corrida_info['href_tf'], href_gh=corrida_info['href_gh']
    )
    db.add(nova_corrida)
    db.flush()
    print(f"\n[+] Processando Corrida: {pista.nome} - {nova_corrida.horario} (ID: {nova_corrida.id})")

    blocos_de_galgos = soup_tf.find_all('tbody', class_='rpb')
    for trap_info in blocos_de_galgos:
        try:
            trap_num = int(trap_info.get('data-trap', 0))
            if trap_num == 0: continue

            nome_galgo_el = trap_info.find('a', class_=lambda c: c and c.startswith('rpb-greyhound'))
            if not nome_galgo_el: continue
            
            nome_galgo = nome_galgo_el.get_text(strip=True).title()

            pedigree_els = trap_info.find_all('span', class_='rp-setting-pedigree')
            sire = pedigree_els[0].get_text(strip=True).title() if len(pedigree_els) > 0 else None
            dam = pedigree_els[1].get_text(strip=True).title() if len(pedigree_els) > 1 else None

            galgo = db.query(Galgo).filter(Galgo.nome == nome_galgo, Galgo.sire == sire, Galgo.dam == dam).first()

            if not galgo:
                details_el = trap_info.find('span', title="The greyhound's gender, colour and DOB")
                cor, genero, data_nasc_str = (None, None, None)
                if details_el:
                    parts = details_el.get_text(strip=True).split(' ', 2)
                    cor = parts[0] if len(parts) > 0 else None
                    genero = parts[1] if len(parts) > 1 else None
                    data_nasc_str = parts[2] if len(parts) > 2 else None
                
                galgo = Galgo(
                    nome=nome_galgo, 
                    cor=cor, 
                    genero=genero, 
                    data_nascimento=padronizar_data(data_nasc_str),
                    sire=sire, 
                    dam=dam
                )
                db.add(galgo)
                db.flush()

            win_record_el = trap_info.find('span', title="Greyhound's career win record for this type of racing")
            trap_record_el = trap_info.find('span', title="Greyhound's career win record at this trap for this type of racing")
            mstr_el = trap_info.find('div', class_='rpb-rating rpb-final-rating')
            sect_el = trap_info.find('div', class_='rpb-rating rpb-sectional-rating')
            trainer_full_el = trap_info.find('span', title=lambda t: t and 'trainer' in t)
            seed_el = trap_info.find('span', title="The greyhound's seed")
            posicoes_anteriores_el = trap_info.find('span', title='The previous 5 finishing positions of this greyhound')
            comentario_tf_el = trap_info.find('b', title="Timeform's comment summing up the prospect for each greyhound in this race")

            trainer_nome, trainer_rate = None, None
            if trainer_full_el:
                trainer_full = trainer_full_el.get_text(strip=True)
                if '(' in trainer_full:
                    partes = trainer_full.split(' (', 1)
                    trainer_nome = partes[0].strip().title()
                    trainer_rate = partes[1].replace(')', '').strip() if len(partes) > 1 else None
                else:
                    trainer_nome = trainer_full.strip().title()

            treinador_obj = None
            if trainer_nome:
                treinador_obj = db.query(Treinador).filter(Treinador.nome == trainer_nome).first()
                if not treinador_obj:
                    treinador_obj = Treinador(nome=trainer_nome)
                    db.add(treinador_obj)
                    db.flush()

            nova_participacao = Participacao(
                corrida_id=nova_corrida.id,
                galgo_id=galgo.id,
                faixa=trap_num,
                treinador=treinador_obj,
                strike_rate=trainer_rate,
                posicoes_anteriores=posicoes_anteriores_el.get_text(strip=True) if posicoes_anteriores_el else None,
                comentario_tf=comentario_tf_el.get_text(strip=True) if comentario_tf_el else None,
                win_rec=win_record_el.get_text(strip=True) if win_record_el else None,
                trap_rec=trap_record_el.get_text(strip=True) if trap_record_el else None,
                seed=seed_el.get_text(strip=True) if seed_el else None,
                mstr=int(mstr_el.get_text(strip=True)) if mstr_el and mstr_el.get_text(strip=True).isdigit() else None,
                sect=int(sect_el.get_text(strip=True)) if sect_el and sect_el.get_text(strip=True).isdigit() else None
            )
            db.add(nova_participacao)
            print(f"    -> [TF] Criado participante da Trap {trap_num}: {galgo.nome}")

            dynamic_history_class = f"rpb-recent-form-{trap_num}"
            history_tr = trap_info.find('tr', class_=dynamic_history_class)
            if history_tr:
                recent_form_table = history_tr.find('table', class_='recent-form')
                if recent_form_table and recent_form_table.find('tbody'):
                    for row in recent_form_table.find('tbody').find_all('tr'):
                        if 'run-comment-mob' in row.get('class', []): continue
                        cols = row.find_all('td')
                        if len(cols) < 17: continue

                        data_hist = padronizar_data(cols[0].get_text(strip=True))
                        if not data_hist: continue

                        pista_hist_abreviada = cols[2].get_text(strip=True)
                        pista_hist_nome = mapa_pistas.get(pista_hist_abreviada, pista_hist_abreviada)
                        if pista_hist_nome == pista_hist_abreviada and len(pista_hist_abreviada) <= 5:
                            print(f"    !! Aviso de Mapeamento: Nova abreviação de pista encontrada '{pista_hist_abreviada}'. Considere adicionar ao 'mapa_pistas.json'.")
                        dist_hist_str = cols[3].get_text(strip=True).replace('m', '')
                        chave_unica = (galgo.id, data_hist, pista_hist_nome, dist_hist_str)
                        
                        if chave_unica not in historico_compilado:
                            historico_compilado[chave_unica] = {
                                "galgo_id": galgo.id,
                                "data": data_hist,
                                "pista_nome": pista_hist_nome,
                                "distancia_str": dist_hist_str,
                                "observacoes_tf": cols[0].get('title', '').strip(),
                                "tipo_corrida": cols[1].get_text(strip=True),
                                "categoria": cols[4].get_text(strip=True),
                                "eye": cols[5].get_text(strip=True),
                                "proxy": cols[6].get_text(strip=True),
                                "faixa_str": cols[7].get_text(strip=True),
                                "tf_sec_str": cols[8].get_text(strip=True),
                                "bend_str": cols[9].get_text(strip=True),
                                "fin_str": cols[10].get_text(strip=True),
                                "btn_by": cols[11].get_text(strip=True),
                                "tf_going": cols[12].get_text(strip=True),
                                "isp": cols[13].get_text(strip=True),
                                "tf_time_str": cols[14].get_text(strip=True),
                                "sec_rtg": cols[15].get_text(strip=True),
                                "rtg": cols[16].get_text(strip=True),
                            }

        except Exception as e:
            print(f"    !! Erro ao processar um participante do Timeform. Pulando. Erro: {e}")
            continue

    if soup_gh:
        runner_container = soup_gh.find('div', id='sortContainer', class_='cardTabContainer')
        if runner_container:
            for block in runner_container.find_all('div', class_='runnerBlock'):
                try:
                    trap_el = block.find('i', class_=re.compile(r'\btrap\d+\b'))
                    if not trap_el: continue

                    trap_num_match = re.search(r'trap(\d+)', ' '.join(trap_el['class']))
                    if not trap_num_match: continue

                    trap_num = int(trap_num_match.group(1))
                    participacao_a_atualizar = next((p for p in nova_corrida.participacoes if p.faixa == trap_num), None)

                    if participacao_a_atualizar:
                        comentario_gh_el = block.find('p', class_='comment')
                        if comentario_gh_el:
                            participacao_a_atualizar.comentario_gh = comentario_gh_el.get_text(strip=True)

                        info_table = block.find('table')
                        if info_table:
                            for row in info_table.find_all('tr'):
                                cells = row.find_all('td')
                                if len(cells) >= 2:
                                    left_text = cells[0].get_text(strip=True)
                                    right_text = cells[1].get_text(strip=True)
                                    if 'SP Forecast:' in left_text:
                                        participacao_a_atualizar.sp_forecast = left_text.replace('SP Forecast:', '').strip()
                                    if 'Topspeed:' in right_text:
                                        participacao_a_atualizar.top_speed = right_text.replace('Topspeed:', '').strip()

                        print(f"    -> [GH] Atualizado participante da Trap {trap_num}")

                except Exception as e:
                    print(f"    !! Erro ao processar um runner block do GreyhoundBet. Pulando. Erro: {e}")
                    continue

        try:
            print("    -> Clicando na aba 'Form' para obter mais detalhes...")
            driver.find_element(By.ID, "cardTab-form").click()
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".formTabContainer")))
            time.sleep(1)

            soup_gh_final = BeautifulSoup(driver.page_source, 'html.parser')
            form_container = soup_gh_final.find('div', id='sortContainer', class_='formTabContainer')

            if form_container:
                final_runner_blocks = form_container.find_all('div', class_='runnerBlock')
                for block in final_runner_blocks:
                    trap_el = block.find('i', class_=lambda c: c and c.startswith('trap'))
                    if not trap_el: continue
                    trap_num = int(trap_el['class'][1].replace('trap', ''))

                    participacao_a_atualizar = next(
                        (p for p in nova_corrida.participacoes if p.faixa == trap_num), None
                    )

                    if participacao_a_atualizar:
                        galgo_a_atualizar = participacao_a_atualizar.galgo

                        dog_details = block.find('tr', class_='dogDetails')
                        if dog_details:
                            details_text = dog_details.get_text(strip=True)
                            match = re.search(r'([A-Za-z]+)\s([bdw])\s(.*?)-(.*?)\s([A-Za-z]{3}\d{2})', details_text)
                            if match:
                                gh_cor = match.group(1)
                                gh_genero = match.group(2)
                                gh_sire = match.group(3).strip().title()
                                gh_dam = match.group(4).strip().title()
                                gh_data_nasc = padronizar_data(match.group(5).strip())

                                if galgo_a_atualizar.cor != gh_cor:
                                    print(f"    -> Atualizando COR do Galgo {galgo_a_atualizar.nome}: '{galgo_a_atualizar.cor}' -> '{gh_cor}'")
                                    galgo_a_atualizar.cor = gh_cor

                                if galgo_a_atualizar.genero != gh_genero:
                                    print(f"    -> Atualizando GÊNERO do Galgo {galgo_a_atualizar.nome}: '{galgo_a_atualizar.genero}' -> '{gh_genero}'")
                                    galgo_a_atualizar.genero = gh_genero

                                if not galgo_a_atualizar.sire and gh_sire:
                                    galgo_a_atualizar.sire = gh_sire
                                if not galgo_a_atualizar.dam and gh_dam:
                                    galgo_a_atualizar.dam = gh_dam

                                if not galgo_a_atualizar.data_nascimento and gh_data_nasc:
                                    galgo_a_atualizar.data_nascimento = gh_data_nasc

                        brt_el = block.find('td', class_='brt')
                        if brt_el:
                            full_brt_text = brt_el.get_text(strip=True).replace('BRT:', '').strip()
                            if full_brt_text:
                                parts = full_brt_text.split(' ')
                                if len(parts) >= 1: participacao_a_atualizar.brt = parts[0]
                                if len(parts) >= 2:
                                    brt_date_str = parts[-1].strip('()')
                                    participacao_a_atualizar.brt_date = padronizar_data(brt_date_str)
                        
                        print(f"    -> [GH Form] Enriquecido dados para a Trap {trap_num}")

                        form_table = block.find('table', class_='formGrid')

                        if form_table:
                            for row in form_table.find_all('tr'):
                                cells = row.find_all('td')
                                if len(cells) < 16: continue

                                data_hist_gh = padronizar_data(cells[0].get_text(strip=True))
                                if not data_hist_gh: continue

                                pista_hist_abreviada_gh = cells[1].get_text(strip=True)
                                pista_hist_nome_gh = mapa_pistas.get(pista_hist_abreviada_gh, pista_hist_abreviada_gh)
                                dist_hist_gh = cells[2].get_text(strip=True).replace('m', '')

                                chave_unica = (galgo_a_atualizar.id, data_hist_gh, pista_hist_nome_gh, dist_hist_gh)

                                video_link_intermediario = None
                                video_link_el = cells[0].find('a', class_='videoLink')
                                if video_link_el:
                                    video_link_intermediario = video_link_el.get('href')
                                
                                # Adiciona a tarefa à lista se houver um link
                                if video_link_intermediario:
                                    videos_para_resolver.append({
                                        "chave_unica": chave_unica,
                                        "url_pagina_video": video_link_intermediario
                                    })

                                dados_gh = {
                                    "observacoes_gh": cells[9].get_text(strip=True),
                                    "split": float(cells[4].get_text(strip=True)) if cells[4].get_text(strip=True) else None,
                                    "win_sec": cells[8].get_text(strip=True),
                                    "wntm": float(cells[10].get_text(strip=True)) if cells[10].get_text(strip=True) else None,
                                    "gng": cells[11].get_text(strip=True),
                                    "wght": cells[12].get_text(strip=True),
                                    "caltm": float(cells[15].get_text(strip=True)) if cells[15].get_text(strip=True) else None,
                                    "btn_by_gh": cells[7].get_text(strip=True)
                                }

                                if chave_unica in historico_compilado:
                                    historico_compilado[chave_unica].update(dados_gh)
                                    print(f"    -> [GH Form Hist] Enriquecido histórico da Trap {trap_num}")

        except Exception as e:
            print(f"    !! Aviso: Não foi possível processar a aba 'Form'. Erro: {e}")

    for chave, dados_hist in historico_compilado.items():
        try:
            pista_hist_obj = db.query(Pista).filter(Pista.nome == dados_hist['pista_nome']).first()
            if not pista_hist_obj:
                pista_hist_obj = Pista(nome=dados_hist['pista_nome'])
                db.add(pista_hist_obj)
                db.flush()

            distancia_hist = int(dados_hist['distancia_str']) if dados_hist['distancia_str'].isdigit() else None

            historico_obj = db.query(HistoricoCorrida).filter_by(
                galgo_id=dados_hist['galgo_id'], data=dados_hist['data'],
                pista_id=pista_hist_obj.id, distancia=distancia_hist
            ).first()

            if not historico_obj:
                btn_by_final = dados_hist.get('btn_by') or dados_hist.get('btn_by_gh')

                historico_obj = HistoricoCorrida(
                    galgo_id=dados_hist.get('galgo_id'), data=dados_hist.get('data'),
                    pista_id=pista_hist_obj.id, distancia=distancia_hist,
                    observacoes_tf=dados_hist.get('observacoes_tf'), tipo_corrida=dados_hist.get('tipo_corrida'),
                    categoria=dados_hist.get('categoria'), eye=dados_hist.get('eye'), proxy=dados_hist.get('proxy'),
                    faixa=analisar_posicao_final(dados_hist.get('faixa_str')),
                    fin=analisar_posicao_final(dados_hist.get('fin_str')),
                    bend=analisar_posicao_final(dados_hist.get('bend_str')),
                    tf_going=dados_hist.get('tf_going'), isp=dados_hist.get('isp'),
                    sec_rtg=dados_hist.get('sec_rtg'), rtg=dados_hist.get('rtg'),
                    tf_sec=float(dados_hist['tf_sec_str']) if dados_hist.get('tf_sec_str') else None,
                    tf_time=float(dados_hist['tf_time_str']) if dados_hist.get('tf_time_str') else None,
                    observacoes_gh=dados_hist.get('observacoes_gh'),
                    split=dados_hist.get('split'),
                    win_sec=dados_hist.get('win_sec'),
                    wntm=dados_hist.get('wntm'),
                    gng=dados_hist.get('gng'),
                    wght=dados_hist.get('wght'),
                    caltm=dados_hist.get('caltm'),
                    btn_by=btn_by_final
                )
                db.add(historico_obj)

            if historico_obj not in nova_corrida.historico_snapshot:
                 nova_corrida.historico_snapshot.append(historico_obj)

        except Exception as e:
            print(f"    !! Erro ao salvar um registro de histórico {chave}. Pulando. Erro: {e}")
            continue    

    return True, videos_para_resolver

def resolver_e_atualizar_videos(driver, db, lista_videos, mapa_pistas):
    if not lista_videos:
        print("Nenhuma URL de vídeo para resolver nesta corrida.")
        return

    print(f"Iniciando resolução de {len(lista_videos)} URLs de vídeo...")
    base_url = "https://greyhoundbet.racingpost.com/"

    for tarefa in lista_videos:
        historico_obj = None
        try:
            chave = tarefa["chave_unica"]
            galgo_id, data_corrida, nome_pista, distancia = chave

            pista_obj = db.query(Pista).filter(Pista.nome == nome_pista).first()
            if not pista_obj: continue

            historico_obj = db.query(HistoricoCorrida).filter_by(
                galgo_id=galgo_id, data=data_corrida,
                pista_id=pista_obj.id, distancia=int(distancia)
            ).first()

            if not historico_obj or historico_obj.video_url:
                continue

            url_intermediaria = f"{base_url}{tarefa['url_pagina_video']}"
            driver.get(url_intermediaria)
            wait_intermediaria = WebDriverWait(driver, 10)
            
            video_play_button = wait_intermediaria.until(EC.presence_of_element_located((By.ID, "videoPlayButton")))
            url_pagina_final_video = video_play_button.get_attribute('href')
            
            if not url_pagina_final_video:
                print(f"    !! Aviso: Link da página final do vídeo não encontrado para a tarefa {chave}")
                historico_obj.video_url = "not_found"
                continue

            # Navegação 2: Página Final
            driver.get(url_pagina_final_video)
            wait_final = WebDriverWait(driver, 10)

            video_element = wait_final.until(EC.presence_of_element_located((By.ID, "video")))
            video_url_final = video_element.get_attribute('src')

            if video_url_final:
                historico_obj.video_url = video_url_final
                print(f"    -> Vídeo SRC encontrado e atualizado para o registro de histórico ID {historico_obj.id}")
            else:
                historico_obj.video_url = "not_found"

            time.sleep(random.uniform(1, 2))

        except (TimeoutException, NoSuchElementException):
            print(f"    !! Aviso: Elemento esperado não foi encontrado a tempo para a tarefa {tarefa.get('chave_unica')}")
            if historico_obj:
                historico_obj.video_url = "not_found"
            continue
        except Exception as e:
            print(f"    !! Erro ao processar vídeo para a tarefa {tarefa.get('chave_unica')}. Erro: {e}")
            continue

    db.commit()
    print("Resolução de vídeos finalizada.")

def main():

    try:
        with open('mapa_pistas.json', 'r') as f:
            mapa_pistas = json.load(f)
    except FileNotFoundError:
        print("!!!!!! ERRO CRÍTICO: Arquivo 'mapa_pistas.json' não encontrado. Abortando.")
        return

    driver = configurar_driver()
    try:
        corridas_para_processar = extrair_corridas_iniciais(driver)

        for corrida_info in corridas_para_processar:
            db_gen = get_db()
            db = next(db_gen)

            processamento_bem_sucedido = False

            try:
                corrida_existente = db.query(Corrida).filter(
                    Corrida.href_tf == corrida_info['href_tf']
                ).first()

                if corrida_existente:
                    print(f"Corrida já existe no banco. Pulando: {corrida_info['pista']} {corrida_info['horario']}")
                    continue

                processamento_bem_sucedido, videos_para_resolver = processar_detalhes_corrida(driver, corrida_info, db, mapa_pistas)

                if processamento_bem_sucedido:
                    db.commit()
                    print(f"-> Corrida salva com sucesso.")

                    if videos_para_resolver:
                        resolver_e_atualizar_videos(driver, db, videos_para_resolver, mapa_pistas)

                else:
                    print(f"-> Falha ao processar. Rollback executado.")
                    db.rollback()

            except Exception as e:
                print(f"!!!!!! Erro crítico ao processar a corrida {corrida_info['href_tf']}. Rollback executado. Erro: {e}")
                db.rollback()
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass

                pausa = random.uniform(2, 5)
                print(f"Pausando por {pausa:.2f} segundos...\n")
                time.sleep(pausa)

    except Exception as e:
        print(f"Ocorreu um erro geral no processo de scraping: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()