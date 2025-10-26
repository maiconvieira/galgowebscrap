import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import time
import random

from app.db.conexao import get_db, SessionLocal
from app.db.modelos import HistoricoCorrida # Importa nosso modelo
from app.scrapers.gh_scraper import resolver_um_video # Poderíamos mover a função para cá
from app.core import config

# ... (Configuração de Logging igual ao main.py) ...
# ... (Função configurar_driver() igual ao main.py) ...
# ... (Função worker_raspagem_com_reutilizacao() igual ao main.py) ...
#     (Talvez renomear para worker_video_resolver)

def obter_trabalhos_pendentes_do_db(limite=500):
    """Busca no DB por vídeos que ainda não foram resolvidos."""
    db = SessionLocal()
    try:
        trabalhos = db.query(HistoricoCorrida.id, HistoricoCorrida.video_href_raw).\
            filter(HistoricoCorrida.video_status == 'pending', HistoricoCorrida.video_href_raw != None).\
            limit(limite).all()
        
        # Formata como uma lista de dicts para nosso worker entender
        lista_de_trabalho = [{'id_historico': t.id, 'href_raw': t.video_href_raw} for t in trabalhos]
        
        logging.info(f"Encontrados {len(lista_de_trabalho)} vídeos pendentes para resolver.")
        return lista_de_trabalho
    finally:
        db.close()
    
def resolver_um_video(driver, href_inicial):
    base_url = config.URL_BASE_GH
    
    for attempt in range(3): # Tenta até 3 vezes
        try:
            url_pagina_resultado = f"{base_url}{href_inicial}"
            driver.get(url_pagina_resultado)
            wait = WebDriverWait(driver, 10)

            botao_replay = wait.until(EC.element_to_be_clickable((By.ID, "videoPlayButton")))
            href_pagina_video = botao_replay.get_attribute('href')
        
            if not href_pagina_video:
                logging.warning("      -> Link do botão de replay não encontrado.")
                return None

            url_pagina_video = f"{href_pagina_video}"
            driver.get(url_pagina_video)

            video_tag = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.playerContainer video.htmlVideo")))
            src_final = video_tag.get_attribute('src')

            if src_final:
                return src_final
            else:
                logging.warning("      -> Atributo 'src' do vídeo não encontrado.")
                return None

        except StaleElementReferenceException:
            logging.warning(f"      -> Elemento 'stale' encontrado na tentativa {attempt + 1}. Tentando novamente em 1 segundo...")
            time.sleep(1)
            continue

        except (TimeoutException, NoSuchElementException):
            logging.warning("      -> Elemento para resolução de vídeo não encontrado a tempo.")
            return None
        except Exception as e:
            logging.error("      -> Erro inesperado ao resolver vídeo.", exc_info=True)
            return None
            
    logging.error(f"      -> Falhou em resolver o vídeo {href_inicial} após múltiplas tentativas de 'stale element'.")
    return None

def funcao_de_trabalho_video(driver, trabalho: dict, mapa_json=None):
    """
    Esta é a função que o worker vai executar.
    Ela resolve o vídeo E ATUALIZA O BANCO.
    """
    id_historico = trabalho['id_historico']
    href_raw = trabalho['href_raw']
    
    # 1. Resolve o vídeo
    # (A função 'resolver_um_video' precisaria ser
    #  importada ou copiada para este arquivo)
    
    # !! ATENÇÃO: 'resolver_um_video' precisa do 'driver'
    # Vamos assumir que a 'resolver_um_video' foi movida e adaptada
    
    # ... (lógica do resolver_um_video) ...
    # src_final = resolver_um_video(driver, href_raw) 
    src_final = None # Placeholder
    
    # 2. Atualiza o banco com o resultado
    db = SessionLocal()
    try:
        item = db.query(HistoricoCorrida).get(id_historico)
        if not item:
            logging.error(f"ID {id_historico} não encontrado no DB para atualizar.")
            return

        if src_final:
            item.video_url = src_final
            item.video_status = 'resolved'
            logging.info(f"ID {id_historico} resolvido com sucesso.")
        else:
            item.video_status = 'not_found'
            logging.warning(f"ID {id_historico} não encontrou vídeo.")
        
        db.commit()
    except Exception as e:
        logging.error(f"Erro ao atualizar ID {id_historico} no DB.", exc_info=True)
        db.rollback()
    finally:
        db.close()
    
    # Retorna True/False apenas para estatísticas, se quisermos
    return src_final is not None

def main():
    logging.info("### INICIANDO RESOLVEDOR DE VÍDEOS ###")
    
    lista_de_trabalho = obter_trabalhos_pendentes_do_db()
    
    if not lista_de_trabalho:
        logging.info("Nenhum vídeo pendente encontrado. Encerrando.")
        return

    # Podemos usar o *mesmo* processador paralelo, mas com a *nossa* função de trabalho
    #
    
    # resultados = processar_lista_em_paralelo(lista_de_trabalho, funcao_de_trabalho_video, None)
    
    # logging.info(f"Processamento de vídeos concluído. {len(resultados)} vídeos resolvidos.")

if __name__ == "__main__":
    main()