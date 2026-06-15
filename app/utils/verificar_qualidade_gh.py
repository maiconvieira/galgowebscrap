import json
import os
import glob
import logging
import argparse
import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

SCHEMA_GH = {
    'corrida': [
        'pista', 'horario', 'corrida', 'valor', 'categoria', 
        'distancia', 'going_gh', 'forecast', 'tricast', 
        'total_sp_percent', 'participantes_resultado', 'nr'
    ],
    'participante': [
        'posicao_final', 'faixa', 'galgo', 'galgo_url', 
        'cor', 'sex', 'dt_nasc', 'tempo_final_real', 
        'btn_gh', 'sire', 'dam', 'sp_real', 
        'treinador', 'sectional_time', 'remarks_gh'
    ],
    'participante_variavel': ['tempo_final_real', 'btn_gh', 'isp_info']
}

def verificar_arquivo_gh(caminho_arquivo):
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

    campos_performance = {'tempo_final_real', 'sp_real', 'btn_gh', 'sectional_time'}

    for corrida in dados:
        link = corrida.get('href_gh', 'Link Desconhecido')
        erro_nesta_corrida = False

        log_corrida = {
            'link': link,
            'erros_cabecalho': [],
            'erros_participantes': []
        }

        for campo in SCHEMA_GH['corrida']:
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
                campos_para_validar = set(SCHEMA_GH['participante'] + SCHEMA_GH['participante_variavel'])
                if posicao == 1:
                    campos_para_validar.discard('btn_gh')
                elif isinstance(posicao, int) and posicao > 1:
                    campos_para_validar.discard('tempo_final_real')
                else:
                    for cp in campos_performance:
                        campos_para_validar.discard(cp)
                campos_para_validar.discard('isp_info')
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
    print(f"📊 RELATÓRIO QUALIDADE - GREYHOUND BET")
    print(f"{'='*80}")
    
    if not resultados:
        print("Nenhum arquivo GH encontrado.")
        return

    for res in resultados:
        if not res['lista_detalhada']: continue
            
        print(f"\n📁 ARQUIVO: {res['arquivo']}")
        print(f"   Corridas: {res['total_corridas']} | Com Erros: {len(res['lista_detalhada'])}")
        print(f"   {'-'*77}")

        #LIMITER = 50
        for i, item in enumerate(res['lista_detalhada']):
            #if i >= LIMITER:
            #print(f"   ... [Mais {len(res['lista_detalhada']) - LIMITER} corridas omitidas] ...")
            #break

            url_rel = item['link'].lstrip('/')
            url_completa = f"https://greyhoundbet.racingpost.com/{url_rel}"
            if not url_rel.startswith('#'):
                 url_completa = f"https://greyhoundbet.racingpost.com/#{url_rel}"

            print(f"   🔗 {url_completa}")
            if item['erros_cabecalho']:
                print(f"      ❌ [Corrida] Faltando: {', '.join(item['erros_cabecalho'])}")
            for p in item['erros_participantes']:
                print(f"      ⚠️  [Faixa {p['faixa']}] {p['galgo']}: {', '.join(p['campos'])}")
            print("")

    erros_c = defaultdict(int)
    erros_p = defaultdict(int)
    total_corridas = sum(r['total_corridas'] for r in resultados)

    for res in resultados:
        for k, v in res['erros_corrida'].items(): erros_c[k] += v
        for k, v in res['erros_participante'].items(): erros_p[k] += v

    print(f"{'='*80}")
    print(f"RESUMO FINAL (GH) | Corridas Analisadas: {total_corridas}")
    print("TOP ERROS CORRIDA: " + ", ".join([f"{k}({v})" for k,v in sorted(erros_c.items(), key=lambda x:x[1], reverse=True)[:5]]))
    print("TOP ERROS PARTICIPANTE: " + ", ".join([f"{k}({v})" for k,v in sorted(erros_p.items(), key=lambda x:x[1], reverse=True)[:5]]))

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Verifica dados do GreyhoundBet (GH).")
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

    print(f"Iniciando verificação GH de {d_ini} a {d_fim}...")

    while curr <= d_fim:
        padrao = os.path.join(PASTA_DADOS, f"{curr.strftime('%Y-%m-%d')}_*_gh.json")
        arquivos = glob.glob(padrao)

        for arq in arquivos:
            if 'links' in arq or 'CLEAN' in arq: continue
            res = verificar_arquivo_gh(arq)
            if res: resultados.append(res)
        curr += datetime.timedelta(days=1)

    imprimir_relatorio(resultados)