import json
import os
import glob
import logging
import argparse
import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

# Schema exclusivo para Timeform
SCHEMA_TF = {
    'corrida': [
        'pista', 'horario', 'categoria', 'distancia', 'tipo_corrida', 
        'going_tf', 'going', 'premios', 'forecast', 'tricast', 
        'cartao_corrida'
    ],
    'participante': [
        'posicao_final', 'faixa', 'galgo', 'galgo_url', 
        'btn', 'age', 'sex', 'bend', 'comment', 'sp_real', 
        'tfr', 'tempo_final_real', 'trainer', 'sectional_time', 'bsp'
    ],
    'participante_variavel': ['btn', 'isp_info']
}

def verificar_arquivo_tf(caminho_arquivo):
    nome_arquivo = os.path.basename(caminho_arquivo)

    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)
    except Exception as e:
        logger.error(f"❌ Erro ao ler {nome_arquivo}: {e}")
        return None
    
    estatisticas = {
        'arquivo': nome_arquivo,
        'total_corridas': len(dados),
        'total_participantes': 0,
        'erros_corrida': defaultdict(int),
        'erros_participante': defaultdict(int),
        'lista_detalhada': []
    }

    # Campos que ignoramos se o galgo não terminou (DNF)
    campos_performance = {'tempo_final_real', 'sp_real', 'btn', 'sectional_time', 'bsp', 'tfr'}

    for corrida in dados:
        link = corrida.get('href_tf', 'Link Desconhecido')
        erro_nesta_corrida = False

        log_corrida = {
            'link': link,
            'erros_cabecalho': [],
            'erros_participantes': []
        }

        # 1. Validação Nível Corrida
        for campo in SCHEMA_TF['corrida']:
            valor = corrida.get(campo)
            if valor in [None, "", [], {}]:
                estatisticas['erros_corrida'][campo] += 1
                log_corrida['erros_cabecalho'].append(campo)
                erro_nesta_corrida = True

        participantes = corrida.get('participantes_resultado', [])
        if not participantes:
            estatisticas['erros_corrida']['SEM_PARTICIPANTES'] += 1
            log_corrida['erros_cabecalho'].append('SEM_PARTICIPANTES')
            erro_nesta_corrida = True
        else:
            for p in participantes:
                estatisticas['total_participantes'] += 1
                posicao = p.get('posicao_final')
                faixa = p.get('faixa', '?')

                campos_para_validar = set(SCHEMA_TF['participante'] + SCHEMA_TF['participante_variavel'])

                # --- LÓGICA DE NEGÓCIO TF ---
                if posicao == 1:
                    # Vencedor: Não tem 'btn'
                    campos_para_validar.discard('btn')
                
                elif isinstance(posicao, int) and posicao > 1:
                    # Perdedor: Timeform geralmente dá o tempo real para todos? 
                    # Se não der, descomente a linha abaixo:
                    # campos_para_validar.discard('tempo_final_real')
                    pass
                
                else:
                    # DNF
                    for cp in campos_performance:
                        campos_para_validar.discard(cp)

                # Isp_info é opcional
                campos_para_validar.discard('isp_info')

                # Validação
                campos_faltantes = []
                for campo in campos_para_validar:
                    valor = p.get(campo)
                    if valor in [None, "", []]:
                        estatisticas['erros_participante'][campo] += 1
                        campos_faltantes.append(campo)
                
                if campos_faltantes:
                    erro_nesta_corrida = True
                    log_corrida['erros_participantes'].append({
                        'faixa': faixa,
                        'galgo': p.get('galgo', 'Desconhecido'),
                        'campos': campos_faltantes
                    })

        if erro_nesta_corrida:
            estatisticas['lista_detalhada'].append(log_corrida)

    return estatisticas

def imprimir_relatorio(resultados):
    print(f"\n{'='*80}")
    print(f"📊 RELATÓRIO QUALIDADE - TIMEFORM")
    print(f"{'='*80}")
    
    if not resultados:
        print("Nenhum arquivo TF encontrado.")
        return

    for res in resultados:
        if not res['lista_detalhada']: continue
            
        print(f"\n📁 ARQUIVO: {res['arquivo']}")
        print(f"   Corridas: {res['total_corridas']} | Com Erros: {len(res['lista_detalhada'])}")
        print(f"   {'-'*77}")

        LIMITER = 50
        for i, item in enumerate(res['lista_detalhada']):
            if i >= LIMITER:
                print(f"   ... [Mais {len(res['lista_detalhada']) - LIMITER} corridas omitidas] ...")
                break

            # Monta URL específica do TF
            url_rel = item['link'].lstrip('/')
            url_completa = f"https://www.timeform.com/{url_rel}"

            print(f"   🔗 {url_completa}")
            
            if item['erros_cabecalho']:
                print(f"      ❌ [Corrida] Faltando: {', '.join(item['erros_cabecalho'])}")
            
            for p in item['erros_participantes']:
                print(f"      ⚠️  [Faixa {p['faixa']}] {p['galgo']}: {', '.join(p['campos'])}")
            
            print("")
    
    # Resumo
    erros_c = defaultdict(int)
    erros_p = defaultdict(int)
    total_corridas = sum(r['total_corridas'] for r in resultados)
    
    for res in resultados:
        for k, v in res['erros_corrida'].items(): erros_c[k] += v
        for k, v in res['erros_participante'].items(): erros_p[k] += v

    print(f"{'='*80}")
    print(f"RESUMO FINAL (TF) | Corridas Analisadas: {total_corridas}")
    print("TOP ERROS CORRIDA: " + ", ".join([f"{k}({v})" for k,v in sorted(erros_c.items(), key=lambda x:x[1], reverse=True)[:5]]))
    print("TOP ERROS PARTICIPANTE: " + ", ".join([f"{k}({v})" for k,v in sorted(erros_p.items(), key=lambda x:x[1], reverse=True)[:5]]))

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Verifica dados do Timeform (TF).")
    parser.add_argument('--data_inicio', type=str, required=True, help="AAAA-MM-DD")
    parser.add_argument('--data_fim', type=str, required=True, help="AAAA-MM-DD")
    args = parser.parse_args()

    try:
        d_ini = datetime.datetime.strptime(args.data_inicio, '%Y-%m-%d').date()
        d_fim = datetime.datetime.strptime(args.data_fim, '%Y-%m-%d').date()
    except ValueError:
        print("Erro: Formato de data inválido.")
        exit(1)

    PASTA_DADOS = "data"
    resultados = []
    curr = d_ini
    
    print(f"Iniciando verificação TF de {d_ini} a {d_fim}...")
    
    while curr <= d_fim:
        # Busca apenas arquivos TF
        padrao = os.path.join(PASTA_DADOS, f"{curr.strftime('%Y-%m-%d')}_*_tf.json")
        arquivos = glob.glob(padrao)
        
        for arq in arquivos:
            if 'links' in arq or 'CLEAN' in arq: continue
            res = verificar_arquivo_tf(arq)
            if res: resultados.append(res)
        curr += datetime.timedelta(days=1)

    imprimir_relatorio(resultados)