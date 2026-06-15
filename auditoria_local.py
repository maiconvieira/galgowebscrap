import os
import json
import logging
from collections import Counter
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("auditoria_local.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def executar_auditoria_completa(diretorio_data, data_inicio, data_fim):
    logging.info(f"### INICIANDO AUDITORIA UNIFICADA (TESTE 1 E TESTE 2) ###")
    logging.info(f"Período: {data_inicio} até {data_fim}")
    
    delta = data_fim - data_inicio

    for i in range(delta.days + 1):
        data_alvo = data_inicio + timedelta(days=i)
        data_str = data_alvo.strftime('%Y-%m-%d')

        for sufixo, chave in [("gh", "href_gh"), ("tf", "href_tf")]:
            arq_links = os.path.join(diretorio_data, f"{data_str}_links_1_{sufixo}.json")
            arq_scraped = os.path.join(diretorio_data, f"{data_str}_scraped_1_{sufixo}.json")

            if not os.path.exists(arq_links) or not os.path.exists(arq_scraped):
                continue

            logging.info(f"--- Auditando {sufixo.upper()} para a data: {data_str} ---")

            # ==========================================
            # 1. LEITURA E PARIDADE DE CONJUNTOS (SETs)
            # ==========================================
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
                logging.warning(f"[{data_str} | {sufixo.upper()}] Falha de Paridade Ficheiro Link vs Scraped.")
                if omissao_no_scraped:
                    logging.warning(f"  [-] Consta em LINKS, mas FALTA no SCRAPED: {len(omissao_no_scraped)} itens.")
                if excesso_no_scraped:
                    logging.warning(f"  [+] Consta no SCRAPED, mas NÃO mapeado em LINKS: {len(excesso_no_scraped)} itens.")
            else:
                logging.info(f"[{data_str} | {sufixo.upper()}] Paridade de mapeamento interna: 100%.")

            # ==========================================
            # 2. TESTE 1: VERIFICAÇÃO E REMOÇÃO DE DUPLICATAS
            # ==========================================
            contagem_scraped = Counter(lista_scraped)
            duplicados = {k: v for k, v in contagem_scraped.items() if v > 1}
            
            if duplicados:
                logging.error(f"[{data_str} | {sufixo.upper()}] TESTE 1 FALHOU: {len(duplicados)} links duplicados detectados. Iniciando deduplicação.")
                
                dados_scraped_limpos = []
                chaves_vistas = set()

                for corrida in dados_scraped:
                    link_corrida = corrida.get(chave)
                    if link_corrida not in chaves_vistas:
                        dados_scraped_limpos.append(corrida)
                        chaves_vistas.add(link_corrida)

                dados_scraped = dados_scraped_limpos
                
                logging.info(f"[{data_str} | {sufixo.upper()}] Deduplicação concluída. Total de corridas ajustado para: {len(dados_scraped)}.")

                with open(arq_scraped, 'w', encoding='utf-8') as f:
                    json.dump(dados_scraped, f, indent=4, ensure_ascii=False)
                logging.info(f"[{data_str} | {sufixo.upper()}] Arquivo JSON reescrito sem duplicatas.")

            else:
                logging.info(f"[{data_str} | {sufixo.upper()}] TESTE 1 OK: Nenhuma duplicata encontrada.")

            # ==========================================
            # 3. TESTE 2: AUDITORIA ESTRUTURAL E TIPAGEM
            # ==========================================
            tipos_chaves_raiz = {}
            tipos_chaves_participantes = {}

            for idx, corrida in enumerate(dados_scraped):
                for k, v in corrida.items():
                    if k == 'participantes_resultado':
                        continue
                    tipo_atual = type(v).__name__ if v is not None else "NoneType"
                    if k not in tipos_chaves_raiz:
                        tipos_chaves_raiz[k] = set()
                    tipos_chaves_raiz[k].add(tipo_atual)

                participantes = corrida.get('participantes_resultado', [])
                if not participantes:
                    logging.warning(f"[{data_str} | {sufixo.upper()}] Corrida índice {idx} ({corrida.get(chave)}) sem participantes_resultado.")
                
                for part in participantes:
                    for k_p, v_p in part.items():
                        tipo_p_atual = type(v_p).__name__ if v_p is not None else "NoneType"
                        if k_p not in tipos_chaves_participantes:
                            tipos_chaves_participantes[k_p] = set()
                        tipos_chaves_participantes[k_p].add(tipo_p_atual)

            anomalias_detectadas = False
            for campo, tipos in tipos_chaves_raiz.items():
                if len(tipos - {"NoneType"}) > 1:
                    logging.error(f"[{data_str} | {sufixo.upper()}] TESTE 2 FALHOU (Raiz): Campo '{campo}' mudou de tipo dinamicamente: {list(tipos)}")
                    anomalias_detectadas = True
            
            for campo_p, tipos_p in tipos_chaves_participantes.items():
                if len(tipos_p - {"NoneType"}) > 1:
                    logging.error(f"[{data_str} | {sufixo.upper()}] TESTE 2 FALHOU (Participante): Campo '{campo_p}' variou tipos: {list(tipos_p)}")
                    anomalias_detectadas = True

            if not anomalias_detectadas:
                logging.info(f"[{data_str} | {sufixo.upper()}] TESTE 2 OK: Consistência do esquema validada com sucesso.")

    logging.info(f"### AUDITORIA UNIFICADA CONCLUÍDA ###")

if __name__ == "__main__":
    PASTA_DADOS = "./data" 

    DATA_DE_INICIO = date(2021, 2, 1)
    DATA_DE_FIM = date(2021, 1, 31)
    #DATA_DE_FIM = date.today()

    executar_auditoria_completa(PASTA_DADOS, DATA_DE_INICIO, DATA_DE_FIM)