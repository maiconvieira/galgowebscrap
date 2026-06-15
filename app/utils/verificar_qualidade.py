import json
import os
import glob
import logging
import argparse
import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

SCHEMA_CONFIG = {   
    'gh': {
        'corrida': ['pista', 'horario', 'corrida', 'valor', 'categoria', 
                             'distancia', 'going_gh', 'forecast', 'tricast', 
                             'total_sp_percent', 'participantes_resultado'],
        'participante': ['posicao_final', 'faixa', 'galgo', 'galgo_url', 'cor', 'sex', 'dt_nasc', 
                         'sire', 'dam', 'sp_real', 'treinador', 'sectional_time', 'remarks_gh'],
        'participante_variavel': ['tempo_final_real', 'btn_gh', 'isp_info']
    },
    'tf': {
        'corrida': ['pista', 'horario', 'categoria', 'distancia', 'tipo_corrida', 
                             'going_tf', 'going', 'premios', 'forecast', 'tricast', 
                             'cartao_corrida'],
        'participante': ['posicao_final', 'faixa', 'galgo', 'galgo_url', 'btn', 'age', 'sex', 'bend', 
                         'comment', 'sp_real', 'tfr', 'tempo_final_real', 'trainer', 'sectional_time', 'bsp'],
        'participante_variavel': []
    }
}

def verificar_arquivo(caminho_arquivo):
    nome_arquivo = os.path.basename(caminho_arquivo)

    tipo = 'gh' if '_gh.json' in nome_arquivo else 'tf' if '_tf.json' in nome_arquivo else None
    if not tipo:
        return None

    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)
    except Exception as e:
        logger.error(f"❌ Erro ao ler {nome_arquivo}: {e}")
        return None
    
    estatisticas = {
        'arquivo': nome_arquivo,
        'tipo': tipo,
        'total_corridas': len(dados),
        'total_participantes': 0,
        'erros_corrida': defaultdict(int),
        'erros_participante': defaultdict(int),
        'lista_detalhada': []
    }

    schema = SCHEMA_CONFIG[tipo]
    campos_performance = {'tempo_final_real', 'sp_real', 'btn', 'btn_gh', 'sectional_time', 'bsp', 'isp_info'}

    for corrida in dados:
        link = corrida.get(f'href_{tipo}', 'Link Desconhecido')
        erro_nesta_corrida = False

        log_corrida = {
            'link': link,
            'erros_cabecalho': [],
            'erros_participantes': []
        }

        for campo in schema['corrida']:
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

                campos_para_validar = set(schema['participante'] + schema['participante_variavel'])

                if posicao == 1:
                    if 'btn_gh' in campos_para_validar: 
                        campos_para_validar.remove('btn_gh')
                        campos_para_validar.remove('isp_info')
                    #if 'btn' in campos_para_validar: campos_para_validar.remove('btn')
                elif isinstance(posicao, int) and posicao > 1:
                    if 'tempo_final_real' in campos_para_validar:
                        campos_para_validar.remove('tempo_final_real')
                        #campos_para_validar.remove('isp_info')
                else:
                    campos_para_validar -= campos_performance

                campos_faltantes_participante = []
                for campo in campos_para_validar:
                    valor = p.get(campo)
                    if valor in [None, "", []]:
                        estatisticas['erros_participante'][campo] += 1
                        campos_faltantes_participante.append(campo)

                if campos_faltantes_participante:
                    erro_nesta_corrida = True
                    log_corrida['erros_participantes'].append({
                        'faixa': faixa,
                        'galgo': p.get('galgo', 'Desconhecido'),
                        'campos': campos_faltantes_participante
                    })

        if erro_nesta_corrida:
            estatisticas['lista_detalhada'].append(log_corrida)

    return estatisticas

def imprimir_relatorio_consolidado(resultados_totais):
    print(f"\n{'='*80}")
    print(f"📊 RELATÓRIO DETALHADO DE QUALIDADE DE DADOS")
    print(f"{'='*80}")
    
    for res in resultados_totais:
        if not res['lista_detalhada']:
            continue
            
        print(f"\n📁 ARQUIVO: {res['arquivo']}")
        print(f"   Corridas: {res['total_corridas']} | Com Erros: {len(res['lista_detalhada'])}")
        print(f"   {'-'*77}")

        for i, item in enumerate(res['lista_detalhada']):
            url_completa = item['link']
            if res.get('tipo') == 'gh':
                # Evita barra duplicada se o link já vier com /
                caminho = item['link'] if not item['link'].startswith('/') else item['link']
                url_completa = f"https://greyhoundbet.racingpost.com/{caminho}"
            elif res.get('tipo') == 'tf':
                caminho = item['link'] if not item['link'].startswith('/') else item['link']
                url_completa = f"https://www.timeform.com{caminho}"
            
            print(f"   🔗 Link: {url_completa}")
            
            # Erros do cabeçalho da corrida
            if item['erros_cabecalho']:
                print(f"      ❌ [Corrida] Faltando: {', '.join(item['erros_cabecalho'])}")
            
            # Erros dos participantes
            for p in item['erros_participantes']:
                print(f"      ⚠️  [Faixa {p['faixa']}] {p['galgo']}: {', '.join(p['campos'])}")
            
            if item['erros_cabecalho'] or item['erros_participantes']:
                print("")

    print(f"{'='*80}")
    print(f"📊 RESUMO ESTATÍSTICO CONSOLIDADO")
    print(f"{'='*80}")

    total_files = len(resultados_totais)
    total_corridas = sum(r['total_corridas'] for r in resultados_totais)
    total_parts = sum(r['total_participantes'] for r in resultados_totais)

    # Agregando erros
    erros_corrida_agg = defaultdict(int)
    erros_part_agg = defaultdict(int)

    for res in resultados_totais:
        for k, v in res['erros_corrida'].items():
            erros_corrida_agg[k] += v
        for k, v in res['erros_participante'].items():
            erros_part_agg[k] += v

    print(f"Arquivos: {total_files} | Total Corridas: {total_corridas} | Total Participantes: {total_parts}")

    print("\n🚨 TOP ERROS EM CORRIDAS:")
    if not erros_corrida_agg: print("   ✅ Nenhum erro.")
    for k, v in sorted(erros_corrida_agg.items(), key=lambda x: x[1], reverse=True):
        pct = (v / total_corridas * 100) if total_corridas > 0 else 0
        print(f"   ❌ {k:<20} : {v:>6} ({pct:>5.1f}%)")

    print("\n🚨 TOP ERROS EM PARTICIPANTES:")
    if not erros_part_agg: print("   ✅ Nenhum erro.")
    for k, v in sorted(erros_part_agg.items(), key=lambda x: x[1], reverse=True):
        pct = (v / total_parts * 100) if total_parts > 0 else 0
        tag_critico = "⚠️ CRÍTICO" if k in ['posicao_final', 'galgo', 'faixa'] else ""
        print(f"   ❌ {k:<20} : {v:>6} ({pct:>5.1f}%) {tag_critico}")

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Verifica qualidade dos dados JSON (TF e GH).")
    parser.add_argument('--data_inicio', type=str, required=True, help="AAAA-MM-DD")
    parser.add_argument('--data_fim', type=str, required=True, help="AAAA-MM-DD")
    args = parser.parse_args()

    try:
        data_inicio = datetime.datetime.strptime(args.data_inicio, '%Y-%m-%d').date()
        data_fim = datetime.datetime.strptime(args.data_fim, '%Y-%m-%d').date()
    except ValueError:
        print("Erro no formato da data.")
        exit(1)

    PASTA_DADOS = "data"
    data_atual = data_inicio
    resultados = []

    print(f"Iniciando análise detalhada de {data_inicio} até {data_fim}...")

    while data_atual <= data_fim:
        data_str = data_atual.strftime('%Y-%m-%d')
        padrao_busca = os.path.join(PASTA_DADOS, f"{data_str}_*_*.json")
        arquivos = glob.glob(padrao_busca)

        for arq in arquivos:
            if 'links' in arq: continue
            
            res = verificar_arquivo(arq)
            if res:
                resultados.append(res)
        
        data_atual += datetime.timedelta(days=1)

    if resultados:
        imprimir_relatorio_consolidado(resultados)
    else:
        print("Nenhum arquivo de dados encontrado no período.")