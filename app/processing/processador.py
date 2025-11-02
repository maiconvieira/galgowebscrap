import os
import re
import json
import logging
from datetime import date, datetime
from dotenv import load_dotenv
from pydantic import ValidationError
from sqlalchemy.orm import Session
from sqlalchemy import text

# Imports da nossa estrutura 'app'
from app.core import config
from app.core.helpers import _converter_fracao_para_float, json_serializador_de_data
from app.db.conexao import SessionLocal, engine, Base
from app.db.modelos import Corrida, Participante, HistoricoCorrida, Pista, Treinador, Galgo
from app.core.schemas import CorridaCompleta, ParticipanteBase

FAVORITOS_TF_REGEX = re.compile(r'([\d/]+)\s+([^,]+)')
load_dotenv()

def criar_tabelas():
    try:
        db_user = os.getenv("DB_USER")
        if not db_user:
            logging.warning("DB_USER não encontrado no .env, usando 'postgres' como fallback.")
            db_user = "postgres"

        logging.warning(f"Forçando a recriação do schema 'public' (DROP...CASCADE)...")

        with engine.begin() as connection:
            connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE;"))
            connection.execute(text("CREATE SCHEMA public;"))
            connection.execute(text(f"GRANT ALL ON SCHEMA public TO {db_user};"))
            connection.execute(text(f"GRANT ALL ON SCHEMA public TO public;"))
            connection.execute(text(f"ALTER USER {db_user} SET search_path = public;"))
            logging.info(f"Caminho de busca ('search_path') definido para 'public' para o usuário {db_user}.")

        logging.info("Schema 'public' recriado com sucesso.")
        logging.info("Criando novas tabelas a partir dos modelos...")
        Base.metadata.create_all(bind=engine)
        logging.info("Tabelas criadas com sucesso.")

    except Exception as e:
        logging.critical(f"Falha CRÍTICA ao tentar recriar o schema: {e}", exc_info=True)
        raise

def carregar_dados_json(data_alvo: date) -> tuple[list, list]:
    data_str = data_alvo.strftime('%Y-%m-%d')
    arquivo_tf = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_tf.json")
    arquivo_gh = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_scraped_gh.json")

    dados_tf = []
    dados_gh = []

    try:
        with open(arquivo_tf, 'r', encoding='utf-8') as f:
            dados_tf = json.load(f)
        logging.info(f"Carregados {len(dados_tf)} registros do arquivo TF.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.warning(f"Não foi possível carregar o arquivo Timeform '{arquivo_tf}'. Erro: {e}")

    try:
        with open(arquivo_gh, 'r', encoding='utf-8') as f:
            dados_gh = json.load(f)
        logging.info(f"Carregados {len(dados_gh)} registros do arquivo GH.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.warning(f"Não foi possível carregar o arquivo Greyhound '{arquivo_gh}'. Erro: {e}")
        
    return dados_tf, dados_gh

def mesclar_dados_scraped(dados_tf: list, dados_gh: list, data_alvo: date) -> list:
    logging.info("Iniciando mesclagem dos dados TF e GH...")
    mapa_gh = {}

    for corrida_gh in dados_gh:
        pista_gh_norm = corrida_gh.get('pista')
        horarios_gh_norm = normalizar_horario_gh_para_tf(corrida_gh.get('horario'))
        
        for horario_norm in horarios_gh_norm:
            chave = (pista_gh_norm, horario_norm)
            if chave not in mapa_gh:
                mapa_gh[chave] = corrida_gh
            else:
                logging.warning(f"Duplicidade de chave GH encontrada: {chave}")

    corridas_mescladas = []
    corridas_tf_descartadas = 0

    for corrida_tf in dados_tf:
        pista_tf_norm = corrida_tf.get('pista')
        horario_tf = corrida_tf.get('horario')
        chave_busca = (pista_tf_norm, horario_tf)
        corrida_gh_correspondente = mapa_gh.pop(chave_busca, {})

        if not corrida_gh_correspondente:
            corridas_tf_descartadas += 1
            continue

        dados_mesclados = {**corrida_gh_correspondente, **corrida_tf}
        dados_mesclados['data_corrida'] = data_alvo

        mapa_p_gh = {p.get('faixa'): p for p in corrida_gh_correspondente.get('participantes', []) if p.get('faixa')}
        participantes_mesclados = []

        for p_tf in corrida_tf.get('participantes', []):
            faixa_tf = p_tf.get('faixa')
            if not faixa_tf:
                continue

            p_gh = mapa_p_gh.get(faixa_tf, {})
            p_mesclado = {**p_tf, **p_gh}
            mapa_h_gh = {(h.get('data'), h.get('pista')): h for h in p_gh.get('historico', []) if h.get('data')}

            historico_mesclado = []
            for h_tf in p_tf.get('historico', []):
                chave_h = (h_tf.get('data'), h_tf.get('pista'))
                h_gh = mapa_h_gh.pop(chave_h, {})
                h_mesclado = {**h_tf, **h_gh}
                historico_mesclado.append(h_mesclado)

            historico_mesclado.extend(mapa_h_gh.values())

            p_mesclado['historico'] = historico_mesclado
            participantes_mesclados.append(p_mesclado)

        dados_mesclados['participantes'] = participantes_mesclados
        corridas_mescladas.append(dados_mesclados)

    if corridas_tf_descartadas > 0:
        logging.warning(f"{corridas_tf_descartadas} corridas do TF não encontraram correspondência em GH.")
    if mapa_gh:
        logging.warning(f"{len(mapa_gh)} corridas do GH não encontraram correspondência em TF.")

    logging.info(f"Mesclagem concluída. Total de {len(corridas_mescladas)} corridas para processar.")
    return corridas_mescladas

def processar_e_salvar_dados(data_alvo: date):
    logging.info(f"Iniciando ETL (Processamento T & L) para a data: {data_alvo.strftime('%Y-%m-%d')}")
    data_str = data_alvo.strftime('%Y-%m-%d')

    dados_tf, dados_gh = carregar_dados_json(data_alvo)
    if not dados_tf:
        logging.error("Nenhum dado do Timeform encontrado para {data_alvo}. O processamento será interrompido.")
        if not dados_tf and not dados_gh:
             logging.warning(f"Nenhum dado (nem TF, nem GH) encontrado para {data_alvo}.")
             return

    corridas_mescladas = mesclar_dados_scraped(dados_tf, dados_gh, data_alvo)

    caminho_debug_1 = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_debug_1_mesclado_bruto.json")
    try:
        with open(caminho_debug_1, 'w', encoding='utf-8') as f:
            json.dump(corridas_mescladas, f, indent=2, default=json_serializador_de_data)
        logging.info(f"Arquivo de debug 1 (bruto mesclado) salvo em: {caminho_debug_1}")
    except Exception as e_json:
        logging.error(f"Falha ao salvar JSON de debug 1: {e_json}")

    logging.info(f"Aplicando filtros de ML (Categoria e Distância) em {len(corridas_mescladas)} corridas...")
    corridas_filtradas = []
    contagem_descarte = {'distancia': 0, 'categoria': 0}

    for corrida_raw in corridas_mescladas:
        distancia = corrida_raw.get('distancia')
        if not (distancia and config.DISTANCIA_MINIMA <= distancia <= config.DISTANCIA_MAXIMA):
            contagem_descarte['distancia'] += 1

        categoria = str(corrida_raw.get('categoria', '')).upper()
        if categoria not in config.CATEGORIAS_PERMITIDAS:
            contagem_descarte['categoria'] += 1
            continue

        corridas_filtradas.append(corrida_raw)

    logging.info(f"Filtragem concluída. {len(corridas_filtradas)} corridas são elegíveis para o ML.")

    if contagem_descarte['distancia'] > 0:
        logging.warning(f"  -> {contagem_descarte['distancia']} corridas descartadas por distância (fora de {config.DISTANCIA_MINIMA}m-{config.DISTANCIA_MAXIMA}m).")
    if contagem_descarte['categoria'] > 0:
        logging.warning(f"  -> {contagem_descarte['categoria']} corridas descartadas por categoria (fora de {config.CATEGORIAS_PERMITIDAS}).")

    corridas_validadas_pydantic = []
    corridas_validadas_dict = []
    falhas_validacao = 0

    logging.info(f"Iniciando limpeza e validação de {len(corridas_filtradas)} corridas...")
    for dados_corrida in corridas_filtradas:
        try:
            dados_corrida_limpos = reorganizar_corrida_json(dados_corrida)
            corrida_para_features = CorridaCompleta(**dados_corrida_limpos)
            
            # ==========================================================
            # !! AQUI VAI ENTRAR NOSSA LÓGICA DE ML (Próximo Passo) !!
            # from app.ml.feature_engineering import calcular_features_ltr
            # corrida_com_features = calcular_features_ltr(corrida_para_features)
            # ==========================================================

            corrida_com_features = corrida_para_features
            corrida_validada_pydantic = CorridaCompleta(**corrida_com_features.model_dump())
            corridas_validadas_pydantic.append(corrida_validada_pydantic)
            
            corridas_validadas_dict.append(corrida_validada_pydantic.model_dump())
        
        except ValidationError as e:
            href = dados_corrida.get('href_tf', 'N/A')
            logging.error(f"Falha de validação Pydantic para a corrida {href}. Erros: {e}")
            falhas_validacao += 1
        except Exception as e_clean:
            href = dados_corrida.get('href_tf', 'N/A')
            logging.error(f"Falha inesperada ao limpar a corrida {href}.", exc_info=True)
            falhas_validacao += 1

    caminho_debug_2 = os.path.join(config.PASTA_DE_DADOS, f"{data_str}_debug_2_reorganizado_limpo.json")
    try:
        with open(caminho_debug_2, 'w', encoding='utf-8') as f:
            json.dump(corridas_validadas_dict, f, indent=2, default=json_serializador_de_data)
        logging.info(f"Arquivo de debug 2 (limpo/pydantic) salvo em: {caminho_debug_2}")
    except Exception as e_json:
        logging.error(f"Falha ao salvar JSON de debug 2: {e_json}")

    if falhas_validacao > 0:
        logging.warning(f"Houveram {falhas_validacao} falhas de validação/limpeza que foram descartadas.")

    if not corridas_validadas_pydantic:
        logging.error("Nenhuma corrida foi validada com sucesso. Verifique os logs de erro.")
        return

    logging.info(f"Iniciando salvamento de {len(corridas_validadas_pydantic)} corridas no banco de dados...")

    db: Session = SessionLocal() 
    try:
        logging.warning(f"Limpando dados existentes para a data {data_alvo} antes da inserção...")

        query_delete = db.query(Corrida).filter(Corrida.data_corrida == data_alvo)
        num_deletadas = query_delete.delete(synchronize_session=False)
        logging.info(f"{num_deletadas} corridas (e seus participantes/históricos) deletadas para {data_alvo}.")
        
        for corrida_pydantic in corridas_validadas_pydantic:
            pista_obj, _= get_or_create(db, Pista, nome=corrida_pydantic.pista)

            exclude_keys = {
                'participantes', 
                'pista',
                'fav_nome_1_tf', 'fav_nome_2_tf', 'fav_nome_3_tf', 'fav_nome_4_tf', 'fav_nome_5_tf',
                'fav_nome_1_gh', 'fav_nome_2_gh', 'fav_nome_3_gh'
            }

            dados_corrida_db = corrida_pydantic.model_dump(exclude=exclude_keys)
            nova_corrida = Corrida(**dados_corrida_db)
            nova_corrida.pista_id = pista_obj.id

            mapa_nome_para_id_galgo = {}

            for p_pydantic in corrida_pydantic.participantes:
                treinador_obj = None
                if p_pydantic.treinador:
                    treinador_obj, _= get_or_create(db, Treinador, nome=p_pydantic.treinador)

                galgo_obj, _= get_or_create(db, Galgo,
                                          nome=p_pydantic.nome_galgo,
                                          dt_nasc=p_pydantic.dt_nasc)

                if galgo_obj and p_pydantic.nome_galgo:
                    mapa_nome_para_id_galgo[p_pydantic.nome_galgo] = galgo_obj.id

                atualizar_galgo_com_dados_novos(galgo_obj, p_pydantic)

                dados_p_db = p_pydantic.model_dump(exclude={
                    'historico', 'treinador', 'nome_galgo', 'dt_nasc', 'sexo', 'cor', 'sire', 'dam'
                })
                novo_participante = Participante(**dados_p_db)

                novo_participante.treinador_id = treinador_obj.id if treinador_obj else None
                novo_participante.galgo_id = galgo_obj.id

                for h_pydantic in p_pydantic.historico:
                    pista_hist_obj = None
                    if h_pydantic.pista:
                        pista_hist_obj, _ = get_or_create(db, Pista, nome=h_pydantic.pista)
                    
                    dados_h_db = h_pydantic.model_dump(exclude={'pista'})
                    novo_historico = HistoricoCorrida(**dados_h_db)
                    novo_historico.pista_id = pista_hist_obj.id if pista_hist_obj else None
                    
                    novo_participante.historico.append(novo_historico)
                
                nova_corrida.participantes.append(novo_participante)

            if corrida_pydantic.fav_nome_1_tf:
                nova_corrida.fav_galgo_id_1_tf = mapa_nome_para_id_galgo.get(corrida_pydantic.fav_nome_1_tf)
            if corrida_pydantic.fav_nome_2_tf:
                nova_corrida.fav_galgo_id_2_tf = mapa_nome_para_id_galgo.get(corrida_pydantic.fav_nome_2_tf)
            if corrida_pydantic.fav_nome_3_tf:
                nova_corrida.fav_galgo_id_3_tf = mapa_nome_para_id_galgo.get(corrida_pydantic.fav_nome_3_tf)
            if corrida_pydantic.fav_nome_4_tf:
                nova_corrida.fav_galgo_id_4_tf = mapa_nome_para_id_galgo.get(corrida_pydantic.fav_nome_4_tf)
            if corrida_pydantic.fav_nome_5_tf:
                nova_corrida.fav_galgo_id_5_tf = mapa_nome_para_id_galgo.get(corrida_pydantic.fav_nome_5_tf)
            
            if corrida_pydantic.fav_nome_1_gh:
                nova_corrida.fav_galgo_id_1_gh = mapa_nome_para_id_galgo.get(corrida_pydantic.fav_nome_1_gh)
            if corrida_pydantic.fav_nome_2_gh:
                nova_corrida.fav_galgo_id_2_gh = mapa_nome_para_id_galgo.get(corrida_pydantic.fav_nome_2_gh)
            if corrida_pydantic.fav_nome_3_gh:
                nova_corrida.fav_galgo_id_3_gh = mapa_nome_para_id_galgo.get(corrida_pydantic.fav_nome_3_gh)
            
            db.add(nova_corrida)
        
        db.commit()
        logging.info("Dados salvos no banco com sucesso (commit realizado).")
        
    except Exception as e_db:
        logging.error(f"Erro ao salvar dados no banco. Realizando rollback.", exc_info=True)
        db.rollback()
    finally:
        db.close()
        logging.info("Sessão do banco fechada.")

def normalizar_horario_gh_para_tf(horario_gh_str: str) -> list[str]:
    try:
        t = datetime.strptime(horario_gh_str, '%H:%M').time()
        hora = t.hour
        minuto_str = f"{t.minute:02d}"
        if 1 <= hora <= 7:
            hora_pm = hora + 12
            return [f"{hora_pm:02d}:{minuto_str}"]
        elif 11 <= hora <= 12:
            return [horario_gh_str]
        elif 8 <= hora <= 10:
            hora_pm = hora + 12
            horario_am = horario_gh_str
            horario_pm = f"{hora_pm:02d}:{minuto_str}"
            return [horario_am, horario_pm]
        else:
            return [horario_gh_str]
    except (ValueError, TypeError):
        logging.warning(f"Formato de horário GH inválido: {horario_gh_str}")
        return []

def atualizar_galgo_com_dados_novos(galgo_obj: Galgo, p_pydantic: ParticipanteBase):
    if not galgo_obj.sexo and p_pydantic.sexo:
        galgo_obj.sexo = p_pydantic.sexo
    if not galgo_obj.cor and p_pydantic.cor:
        galgo_obj.cor = p_pydantic.cor
    if not galgo_obj.sire and p_pydantic.sire:
        galgo_obj.sire = p_pydantic.sire
    if not galgo_obj.dam and p_pydantic.dam:
        galgo_obj.dam = p_pydantic.dam

def get_or_create(db: Session, model, defaults: dict = None, **kwargs):
    instance = db.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = kwargs.copy()
        if defaults:
            params.update(defaults)
            
        instance = model(**params)
        db.add(instance)
        db.flush() 
        return instance, True

def reorganizar_corrida_json(corrida_mesclada):
    favs_tf = corrida_mesclada.get('favoritos_tf', [])
    favs_gh = corrida_mesclada.get('favoritos_gh', [])

    favs_tf.extend([{}] * (5 - len(favs_tf)))
    favs_gh.extend([{}] * (3 - len(favs_gh)))

    fav_tf_data = {
        'fav_faixa_1_tf': favs_tf[0].get('faixa'),
        'fav_nome_1_tf': favs_tf[0].get('nome'),
        'fav_faixa_2_tf': favs_tf[1].get('faixa'),
        'fav_nome_2_tf': favs_tf[1].get('nome'),
        'fav_faixa_3_tf': favs_tf[2].get('faixa'),
        'fav_nome_3_tf': favs_tf[2].get('nome'),
        'fav_faixa_4_tf': favs_tf[3].get('faixa'),
        'fav_nome_4_tf': favs_tf[3].get('nome'),
        'fav_faixa_5_tf': favs_tf[4].get('faixa'),
        'fav_nome_5_tf': favs_tf[4].get('nome'),
    }
    
    fav_gh_data = {
        'fav_faixa_1_gh': favs_gh[0].get('faixa'),
        'fav_nome_1_gh': favs_gh[0].get('nome'),
        'fav_faixa_2_gh': favs_gh[1].get('faixa'),
        'fav_nome_2_gh': favs_gh[1].get('nome'),
        'fav_faixa_3_gh': favs_gh[2].get('faixa'),
        'fav_nome_3_gh': favs_gh[2].get('nome'),
    }

    participantes_limpos = []
    for p_mesclado in corrida_mesclada.get('participantes', []):
        historico_limpo = []
        for h in p_mesclado.get('historico', []):
            sp_raw = h.get('sp')
            sp_odds = _converter_fracao_para_float(sp_raw)
            sp_fav = 1 if (sp_raw and ('F' in sp_raw or 'J' in sp_raw)) else 0
            fin_bruto = h.get('fin')
            fin_limpo = fin_bruto if isinstance(fin_bruto, int) else None

            hist_ordenado = {
                # Chave
                'data': h.get('data'),
                'pista': h.get('pista'),
                'distancia': h.get('distancia'),
                'categoria': h.get('categoria'),
                'tipo_corrida': h.get('tipo_corrida'),
                # Resultados
                'faixa': h.get('faixa'),
                'fin': fin_limpo,
                'btn_tf': h.get('btn_by'),
                'btn_gh': h.get('by'),
                # Tempos
                'split_tf': h.get('tf_sec'),
                'split_gh': h.get('split'),
                'time_tf': h.get('tf_time'),
                'time_gh': h.get('caltm'),
                'time_win': h.get('wntm'),
                # Outros Dados
                'proxy': h.get('proxy'),
                'bend': h.get('bend'),
                'going_tf': h.get('tf_going'),
                'going_gh': h.get('gng'),
                'sec_rtg': h.get('sec_rtg'),
                'rtg': h.get('rtg'),
                'sp_odds': sp_odds,
                'sp_fav': sp_fav,
                'peso': h.get('wght'),
                'video_src': h.get('video_src'),
                'pri_ou_seg': h.get('win_sec'),
                # Comentários
                'observacoes_tf': h.get('observacoes'),
                'observacoes_gh': h.get('remarks')
            }
            historico_limpo.append(hist_ordenado)

        p_limpo = {
            'faixa': p_mesclado.get('faixa'),
            # Info do Galgo
            'nome_galgo': p_mesclado.get('nome_galgo'),
            'dt_nasc': p_mesclado.get('dt_nasc'),
            'cor': p_mesclado.get('cor'),
            'sexo': p_mesclado.get('sexo'),
            'sire': p_mesclado.get('sire'),
            'dam': p_mesclado.get('dam'),
            'form': p_mesclado.get('form'),
            # Info do Treinador
            'treinador': p_mesclado.get('treinador'),
            'strike_rate': p_mesclado.get('strike_rate'),
            # Dados da Participação (Stats)
            'mstr': p_mesclado.get('mstr'),
            'sect': p_mesclado.get('sect'),
            'seed': p_mesclado.get('seed'),
            'win_rec': _converter_fracao_para_float(p_mesclado.get('win_rec')),
            'trap_rec': _converter_fracao_para_float(p_mesclado.get('trap_rec')),
            'sp_forecast': _converter_fracao_para_float(p_mesclado.get('sp_forecast')),
            'topspeed': p_mesclado.get('topspeed'),
            'brt': p_mesclado.get('brt'),
            'categoria_brt': p_mesclado.get('categoria_brt'),
            'data_brt': p_mesclado.get('data_brt'),
            # Comentários (unificados)
            'comentario_tf': p_mesclado.get('comment_tf'),
            'comentario_gh': p_mesclado.get('comment_gh'),
            # Histórico (unificado e ordenado)
            'historico': historico_limpo
        }
        participantes_limpos.append(p_limpo)

    mapa_faixa_nome = {}
    mapa_nome_faixa = {}

    for p in participantes_limpos:
        faixa = p.get('faixa')
        nome = p.get('nome_galgo')

        if faixa:
            mapa_faixa_nome[faixa] = nome
        if nome:
            mapa_nome_faixa[nome.strip().title()] = faixa

    corrida_limpa = {
        # Info Principal da Corrida
        'data_corrida': corrida_mesclada.get('data_corrida'),
        'pista': corrida_mesclada.get('pista'),
        'horario': corrida_mesclada.get('horario'),
        'categoria': corrida_mesclada.get('categoria'),
        'corrida': corrida_mesclada.get('corrida'),
        'distancia': corrida_mesclada.get('distancia'),
        'tipo_corrida': corrida_mesclada.get('tipo_corrida'),
        'premios': corrida_mesclada.get('premios'),
        # Detalhes (TF)
        'perfil_pista': corrida_mesclada.get('perfil_pista'),
        'cartao_corrida': corrida_mesclada.get('cartao_corrida'),
        # Links e Fontes
        'href_tf': corrida_mesclada.get('href_tf'),
        'href_gh': corrida_mesclada.get('href_gh'),
        **fav_tf_data,
        **fav_gh_data
    }

    favoritos_gh_faixas_str = corrida_mesclada.get('post_pick')
    if favoritos_gh_faixas_str:
        try:
            faixas_picks_gh = [int(f) for f in favoritos_gh_faixas_str.split('-')]
            for i, faixa_num in enumerate(faixas_picks_gh, 1):
                corrida_limpa[f'fav_faixa_{i}_gh'] = faixa_num
                corrida_limpa[f'fav_nome_{i}_gh'] = mapa_faixa_nome.get(faixa_num, None)
        except (ValueError, TypeError):
            try:
                nome_limpo_gh = favoritos_gh_faixas_str.strip().title()
                faixa_encontrada_gh = mapa_nome_faixa.get(nome_limpo_gh, None)
                corrida_limpa[f'fav_faixa_1_gh'] = faixa_encontrada_gh
                corrida_limpa[f'fav_nome_1_gh'] = nome_limpo_gh
            except Exception as e:
                logging.warning(f"Falha ao parsear post_pick_gh (formato fallback): {e}")
                pass # Ignora erros
        except Exception as e:
            logging.warning(f"Falha ao parsear post_pick_gh (formato novo): {e}")
            pass
    favoritos_tf_str = corrida_mesclada.get('favoritos')
    if favoritos_tf_str:
        try:
            matches = FAVORITOS_TF_REGEX.findall(favoritos_tf_str)
            
            for i, (odds_str, nome_str) in enumerate(matches, 1):
                nome_limpo = nome_str.strip().title()
                faixa_encontrada = mapa_nome_faixa.get(nome_limpo, None)
                odds_float = _converter_fracao_para_float(odds_str)
                
                corrida_limpa[f'fav_faixa_{i}_tf'] = faixa_encontrada
                corrida_limpa[f'fav_nome_{i}_tf'] = nome_limpo
                corrida_limpa[f'fav_prev_{i}_tf'] = odds_float
        except Exception as e:
            logging.warning(f"Falha ao parsear favoritos: {e}")
            pass
    corrida_limpa['participantes'] = participantes_limpos

    return corrida_limpa