import json
import os
import logging
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    from app.core import config
    PASTA_DE_DADOS = config.PASTA_DE_DADOS
except ImportError:
    logging.warning("Não foi possível importar 'app.core.config'. Usando pasta 'data' como padrão.")
    PASTA_DE_DADOS = "data"


def verificar_links_do_dia(data_alvo_str: str):
    logging.info(f"--- Iniciando verificação de integridade para {data_alvo_str} ---")
    
    erros_encontrados = False

    logging.info("Verificando Greyhound (GH)...")
    gh_links_file = os.path.join(PASTA_DE_DADOS, f"{data_alvo_str}_links_1_gh.json")
    gh_scraped_file = os.path.join(PASTA_DE_DADOS, f"{data_alvo_str}_scraped_1_gh.json")
    
    missing_gh = _comparar_arquivos(gh_links_file, gh_scraped_file, "href_gh")
    
    if missing_gh is None:
        erros_encontrados = True
    elif not missing_gh:
        logging.info("✅ SUCESSO (GH): Todos os links de origem estão no arquivo raspado.")
    else:
        logging.warning(f"🚨 FALHA (GH): {len(missing_gh)} links estão faltando no arquivo raspado:")
        for link in missing_gh:
            logging.warning(f"  - {link}")
        erros_encontrados = True

    logging.info("Verificando Timeform (TF)...")
    tf_links_file = os.path.join(PASTA_DE_DADOS, f"{data_alvo_str}_links_1_tf.json")
    tf_scraped_file = os.path.join(PASTA_DE_DADOS, f"{data_alvo_str}_scraped_1_tf.json")

    missing_tf = _comparar_arquivos(tf_links_file, tf_scraped_file, "href_tf")
    
    if missing_tf is None:
        erros_encontrados = True
    elif not missing_tf:
        logging.info("✅ SUCESSO (TF): Todos os links de origem estão no arquivo raspado.")
    else:
        logging.warning(f"🚨 FALHA (TF): {len(missing_tf)} links estão faltando no arquivo raspado:")
        for link in missing_tf:
            logging.warning(f"  - {link}")
        erros_encontrados = True

    logging.info(f"--- Verificação Concluída para {data_alvo_str} ---")
    return erros_encontrados

def _comparar_arquivos(links_file_path: str, scraped_file_path: str, link_key: str) -> list | None:

    try:
        with open(links_file_path, 'r', encoding='utf-8') as f:
            links_data = json.load(f)
        source_links = {item[link_key] for item in links_data.get("corridas", []) if link_key in item}
    except FileNotFoundError:
        logging.error(f"Arquivo de links não encontrado: {links_file_path}")
        return None
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logging.error(f"Falha ao ler {links_file_path}. Formato inválido? {e}")
        return None

    if not source_links:
        logging.info(f"Nenhum link de origem encontrado em {links_file_path}. Pulando.")
        return []

    try:
        with open(scraped_file_path, 'r', encoding='utf-8') as f:
            scraped_data = json.load(f)
        scraped_links = {item[link_key] for item in scraped_data if isinstance(item, dict) and link_key in item}
    except FileNotFoundError:
        logging.error(f"Arquivo de dados raspados não encontrado: {scraped_file_path}")
        return sorted(list(source_links))
    except (json.JSONDecodeError, TypeError) as e:
        logging.error(f"Falha ao ler {scraped_file_path}. Formato inválido? {e}")
        return None

    missing_links = source_links - scraped_links

    return sorted(list(missing_links))

if __name__ == "__main__":
    
    logging.info("=== INICIANDO VERIFICAÇÃO EM LOTE ===")
    DATA_FIM = date.today()
    #DATA_INICIO = DATA_FIM - timedelta(days=3)
    DATA_FIM = date(2021, 5, 16)
    DATA_INICIO = date(2021, 1, 1)
    
    current_date = DATA_INICIO
    while current_date <= DATA_FIM:
        data_str = current_date.strftime('%Y-%m-%d')
        verificar_links_do_dia(data_str)
        current_date += timedelta(days=1)
    
    logging.info("=== VERIFICAÇÃO EM LOTE CONCLUÍDA ===")
    