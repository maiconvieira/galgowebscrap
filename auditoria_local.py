import os
import json
import logging
import argparse
from collections import Counter
from datetime import date, timedelta, datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("auditoria_local.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def salvar_json_seguro(caminho_arquivo, dados):
    caminho_temp = f"{caminho_arquivo}.tmp"
    with open(caminho_temp, 'w', encoding='utf-8') as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    os.replace(caminho_temp, caminho_arquivo)

def executar_auditoria_completa(diretorio_data, data_inicio, data_fim):
    logging.info(f"INICIANDO AUDITORIA UNIFICADA: {data_inicio} até {data_fim}")    
    delta = data_fim - data_inicio

    for i in range(delta.days + 1):
        data_alvo = data_inicio + timedelta(days=i)
        data_str = data_alvo.strftime('%Y-%m-%d')

        for sufixo, chave in [("gh", "href_gh"), ("tf", "href_tf")]:
            arq_links = os.path.join(diretorio_data, f"{data_str}_links_1_{sufixo}.json")
            arq_scraped = os.path.join(diretorio_data, f"{data_str}_scraped_1_{sufixo}.json")

            if not os.path.exists(arq_links) or not os.path.exists(arq_scraped):
                continue

            try:
                logging.info(f"Auditando {sufixo.upper()} para a data: {data_str}")

                # Teste 1: Paridade e Deduplicação
                with open(arq_links, 'r', encoding='utf-8') as f:
                    dados_links = json.load(f).get('corridas', [])
                set_links = {corrida[chave] for corrida in dados_links if chave in corrida}

                with open(arq_scraped, 'r', encoding='utf-8') as f:
                    dados_scraped = json.load(f)
            
                lista_scraped = [corrida[chave] for corrida in dados_scraped if chave in corrida]
                set_scraped = set(lista_scraped)

                omissao_no_scraped = set_links - set_scraped
                excesso_no_scraped = set_scraped - set_links

                if omissao_no_scraped or excesso_no_scraped:
                        logging.warning(f"[{data_str} | {sufixo.upper()}] Falha de Paridade.")
                        if omissao_no_scraped:
                            logging.warning(f"  [FALTA] Em LINKS, falta no SCRAPED: {len(omissao_no_scraped)}")
                        if excesso_no_scraped:
                            logging.warning(f"  [SOBRA] No SCRAPED, falta em LINKS: {len(excesso_no_scraped)}")

                contagem_scraped = Counter(lista_scraped)
                duplicados = {k: v for k, v in contagem_scraped.items() if v > 1}
                
                if duplicados:
                    logging.error(f"[{data_str} | {sufixo.upper()}] TESTE 1: {len(duplicados)} links duplicados. Deduplicando.")
                    dados_scraped_limpos = []
                    chaves_vistas = set()
                    
                    for corrida in dados_scraped:
                        link_corrida = corrida.get(chave)
                        if link_corrida not in chaves_vistas:
                            dados_scraped_limpos.append(corrida)
                            chaves_vistas.add(link_corrida)

                    salvar_json_seguro(arq_scraped, dados_scraped_limpos)
                    logging.info(f"[{data_str} | {sufixo.upper()}] Arquivo reescrito de forma segura sem duplicatas.")
                    dados_scraped = dados_scraped_limpos

                # Teste 2: Auditoria Estrutural
                tipos_chaves_raiz = {}
                tipos_chaves_participantes = {}

                for corrida in dados_scraped:
                    for k, v in corrida.items():
                        if k == 'participantes_resultado':
                            continue
                        tipo_atual = type(v).__name__ if v is not None else "NoneType"
                        tipos_chaves_raiz.setdefault(k, set()).add(tipo_atual)

                    for part in corrida.get('participantes_resultado', []):
                        for k_p, v_p in part.items():
                            tipo_p_atual = type(v_p).__name__ if v_p is not None else "NoneType"
                            tipos_chaves_participantes.setdefault(k_p, set()).add(tipo_p_atual)

                anomalias = False
                for campo, tipos in tipos_chaves_raiz.items():
                    if len(tipos - {"NoneType"}) > 1:
                        logging.error(f"[{data_str} | {sufixo.upper()}] Campo '{campo}' variou os tipos: {list(tipos)}")
                        anomalias = True
                
                for campo_p, tipos_p in tipos_chaves_participantes.items():
                    if len(tipos_p - {"NoneType"}) > 1:
                        logging.error(f"[{data_str} | {sufixo.upper()}] Participante '{campo_p}' variou os tipos: {list(tipos_p)}")
                        anomalias = True

                if not anomalias:
                    logging.info(f"[{data_str} | {sufixo.upper()}] TESTE 2 OK.")

            except json.JSONDecodeError:
                logging.error(f"O arquivo JSON da data {data_str} está malformado e não pôde ser lido.")
            except Exception as e:
                logging.error(f"Erro ao processar a data {data_str} em {sufixo.upper()}: {e}", exc_info=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Auditoria Local de JSON")
    parser.add_argument("--inicio", type=str, required=True, help="Data de início (YYYY-MM-DD)")
    parser.add_argument("--fim", type=str, required=False, help="Data final (YYYY-MM-DD). Opcional.")
    parser.add_argument("--dir", type=str, default="./data", help="Diretório dos arquivos JSON.")
    args = parser.parse_args()

    data_inicial = datetime.strptime(args.inicio, '%Y-%m-%d').date()
    data_final = datetime.strptime(args.fim, '%Y-%m-%d').date() if args.fim else date.today()

    executar_auditoria_completa(args.dir, data_inicial, data_final)