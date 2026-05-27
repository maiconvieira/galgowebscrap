import json
import os
import logging
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

def carregar_json(caminho):
    if not os.path.exists(caminho):
        return None
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"❌ Erro ao ler {os.path.basename(caminho)}: {e}")
        return None

def encontrar_diferencas(dict1, dict2, caminho=""):
    """
    Compara dois dicionários recursivamente para o relatório de conflitos.
    """
    diferencas = []
    chaves_comuns = set(dict1.keys()) & set(dict2.keys())
    
    for chave in chaves_comuns:
        val1 = dict1[chave]
        val2 = dict2[chave]
        novo_caminho = f"{caminho}.{chave}" if caminho else chave
        
        if isinstance(val1, dict) and isinstance(val2, dict):
            diferencas.extend(encontrar_diferencas(val1, val2, novo_caminho))
        elif isinstance(val1, list) and isinstance(val2, list):
            if val1 != val2:
                diferencas.append(f"Campo '{novo_caminho}' difere em conteúdo de lista")
        else:
            if val1 != val2:
                diferencas.append(f"Campo '{novo_caminho}': '{val1}' != '{val2}'")
                
    return diferencas

def processar_arquivo(caminho_arquivo, chave_unica):
    """
    Processa um único arquivo: remove duplicatas exatas e isola conflitos.
    Retorna estatísticas para o relatório final.
    """
    dados = carregar_json(caminho_arquivo)
    if not dados: return 0, 0 # Retorna 0 processados, 0 conflitos

    # 1. Agrupar por ID
    grupos = defaultdict(list)
    registros_sem_id = []

    for item in dados:
        id_val = item.get(chave_unica)
        if id_val:
            grupos[id_val].append(item)
        else:
            registros_sem_id.append(item)

    registros_limpos = []
    registros_conflitantes = []
    duplicatas_removidas = 0
    
    # 2. Analisar Grupos
    for id_chave, lista_itens in grupos.items():
        if len(lista_itens) == 1:
            registros_limpos.append(lista_itens[0])
        else:
            primeiro = lista_itens[0]
            conflito = False
            
            # Compara o primeiro com o resto
            for outro in lista_itens[1:]:
                if primeiro == outro:
                    duplicatas_removidas += 1
                else:
                    conflito = True
                    diffs = encontrar_diferencas(primeiro, outro)
                    registros_conflitantes.append({
                        "id": id_chave,
                        "total_versoes": len(lista_itens),
                        "diferencas": diffs,
                        "versoes": lista_itens
                    })
                    break 
            
            if not conflito:
                registros_limpos.append(primeiro)

    # 3. Salvar Arquivos
    
    # A) Arquivo Limpo (Sobrescreve ou cria novo sufixo _CLEAN se preferir testar antes)
    # ATENÇÃO: Para "produção", vamos sobrescrever o arquivo original se estiver tudo 100% limpo,
    # mas como pediu relatório de conflito, vou salvar como _CLEAN.json para segurança.
    path_limpo = caminho_arquivo.replace('.json', '_CLEAN.json')
    
    dados_finais = registros_limpos + registros_sem_id
    
    # Só salva se houver alteração (remoção de duplicata ou separação de conflito)
    if duplicatas_removidas > 0 or len(registros_conflitantes) > 0:
        try:
            with open(path_limpo, 'w', encoding='utf-8') as f:
                json.dump(dados_finais, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro ao salvar limpo {path_limpo}: {e}")

    # B) Relatório de Conflitos
    if registros_conflitantes:
        path_conflito = caminho_arquivo.replace('.json', '_CONFLITOS.json')
        try:
            with open(path_conflito, 'w', encoding='utf-8') as f:
                json.dump(registros_conflitantes, f, indent=4, ensure_ascii=False)
            logger.warning(f"   ⚠️  CONFLITO em {os.path.basename(caminho_arquivo)}: {len(registros_conflitantes)} registros.")
        except Exception as e:
             logger.error(f"Erro ao salvar conflito {path_conflito}: {e}")

    if duplicatas_removidas > 0:
        logger.info(f"   ✅ {os.path.basename(caminho_arquivo)}: {duplicatas_removidas} duplicatas exatas removidas.")
        
    return duplicatas_removidas, len(registros_conflitantes)

def processar_intervalo(data_inicio, data_fim, pasta_dados):
    current_date = data_inicio
    total_arquivos = 0
    total_duplicatas = 0
    total_conflitos = 0

    logger.info(f"=== Iniciando Limpeza em Lote: {data_inicio} até {data_fim} ===")

    while current_date <= data_fim:
        data_str = current_date.strftime('%Y-%m-%d')
        
        # Definição dos nomes dos arquivos (padrão do seu projeto)
        # Ajuste aqui se o nome do arquivo mudar (ex: remover o _1_)
        nomes_alvo = [
            (f"{data_str}_scraped_1_gh.json", "href_gh"),
            (f"{data_str}_scraped_1_tf.json", "href_tf")
        ]

        for nome_arq, chave_id in nomes_alvo:
            caminho_completo = os.path.join(pasta_dados, nome_arq)
            
            if os.path.exists(caminho_completo):
                dups, confs = processar_arquivo(caminho_completo, chave_id)
                total_duplicatas += dups
                total_conflitos += confs
                total_arquivos += 1
        
        current_date += timedelta(days=1)

    logger.info(f"\n=== Resumo Final ===")
    logger.info(f"Arquivos Processados: {total_arquivos}")
    logger.info(f"Duplicatas Removidas Automaticamente: {total_duplicatas}")
    logger.info(f"Arquivos com Conflitos (Geraram JSON de auditoria): {total_conflitos}")

if __name__ == "__main__":
    data_ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser(description="Limpa duplicatas em lote.")
    parser.add_argument('--inicio', type=str, default='2021-01-01', help="Data inicial (AAAA-MM-DD)")
    parser.add_argument('--fim', type=str, default=data_ontem, help="Data final (AAAA-MM-DD)")
    parser.add_argument('--pasta', type=str, default='data', help="Pasta onde estão os JSONs")
    
    args = parser.parse_args()

    try:
        d_ini = datetime.strptime(args.inicio, '%Y-%m-%d').date()
        d_fim = datetime.strptime(args.fim, '%Y-%m-%d').date()
    except ValueError:
        logger.error("Erro: Datas devem estar no formato AAAA-MM-DD.")
        exit(1)

    processar_intervalo(d_ini, d_fim, args.pasta)