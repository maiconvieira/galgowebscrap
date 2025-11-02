from pydantic import BaseModel, ConfigDict
from datetime import date
from typing import Optional, List

class BaseConfig(BaseModel):
    model_config = ConfigDict(
        from_attributes=True
    )

class HistoricoCorridaBase(BaseConfig):
    # Chaves de Identificação (O que define a corrida)
    data: Optional[date] = None
    pista: Optional[str] = None
    distancia: Optional[int] = None
    categoria: Optional[str] = None
    tipo_corrida: Optional[str] = None

    # Resultados da Corrida
    faixa: Optional[int] = None
    fin: Optional[int] = None       # Posição final (limpa, 'DN' vira None)
    btn_tf: Optional[str] = None    # Distância do vencedor (Timeform)
    btn_gh: Optional[str] = None    # Distância do vencedor (Greyhound)

    # Dados de Tempo (Splits)
    split_tf: Optional[float] = None
    split_gh: Optional[float] = None
    time_tf: Optional[float] = None # Tempo final (Timeform)
    time_gh: Optional[float] = None # Tempo final (Greyhound)
    time_win: Optional[float] = None # Tempo do vencedor
    
    # Ratings e Odds
    proxy: Optional[int] = None
    bend: Optional[str] = None
    going_tf: Optional[float] = None
    going_gh: Optional[str] = None
    sec_rtg: Optional[int] = None   # Sectional Rating
    rtg: Optional[int] = None       # Rating (TF)
    sp_odds: Optional[float] = None # Odds (convertidas de "5/1" para 0.2)
    sp_fav: Optional[int] = None    # 1 se for favorito, 0 se não
    
    # Outros Dados
    peso: Optional[float] = None
    video_src: Optional[str] = None
    pri_ou_seg: Optional[str] = None
    
    # Comentários
    observacoes_tf: Optional[str] = None
    observacoes_gh: Optional[str] = None

class ParticipanteBase(BaseConfig):
    # Identificação do Galgo
    faixa: int
    nome_galgo: str
    dt_nasc: Optional[date] = None
    cor: Optional[str] = None
    sexo: Optional[str] = None
    sire: Optional[str] = None
    dam: Optional[str] = None
    
    # Identificação do Treinador
    treinador: Optional[str] = None
    
    # Features de LTR (Dados da Corrida)
    # Estes são os dados que o nosso modelo de ML usará
    form: Optional[str] = None
    strike_rate: Optional[float] = None
    mstr: Optional[int] = None
    sect: Optional[int] = None
    seed: Optional[str] = None
    win_rec: Optional[float] = None
    trap_rec: Optional[float] = None
    sp_forecast: Optional[float] = None
    topspeed: Optional[int] = None
    brt: Optional[float] = None
    categoria_brt: Optional[str] = None
    data_brt: Optional[date] = None
    
    # Comentários (para análise de sentimento futura?)
    comentario_tf: Optional[str] = None
    comentario_gh: Optional[str] = None
    
    # ==========================================================
    # !! ENGENHARIA DE FEATURES (PRÓXIMOS PASSOS) !!
    # ==========================================================
    
    # Features Agregadas (que vamos criar)
    # Estas são as features que calcularemos para o LTR
    hist_avg_pos_ult_5: Optional[float] = None
    hist_win_rate_ult_5: Optional[float] = None
    # ... (outras features agregadas) ...
    
    # Dados Brutos de Histórico (para cálculo)
    # O histórico de CADA participante
    historico: List[HistoricoCorridaBase] = []

class CorridaBase(BaseConfig):
    # Identificação da Corrida
    href_tf: str
    href_gh: str
    data_corrida: Optional[date] = None
    pista: Optional[str] = None
    horario: Optional[str] = None
    categoria: Optional[str] = None
    corrida: Optional[int] = None
    distancia: Optional[int] = None
    tipo_corrida: Optional[str] = None
    premios: Optional[str] = None
    perfil_pista: Optional[str] = None
    cartao_corrida: Optional[str] = None
    
    # Dados dos Favoritos (Timeform)
    fav_faixa_1_tf: Optional[int] = None
    fav_nome_1_tf: Optional[str] = None
    fav_prev_1_tf: Optional[float] = None
    fav_faixa_2_tf: Optional[int] = None
    fav_nome_2_tf: Optional[str] = None
    fav_prev_2_tf: Optional[float] = None
    fav_faixa_3_tf: Optional[int] = None
    fav_nome_3_tf: Optional[str] = None
    fav_prev_3_tf: Optional[float] = None
    fav_faixa_4_tf: Optional[int] = None
    fav_nome_4_tf: Optional[str] = None
    fav_prev_4_tf: Optional[float] = None
    fav_faixa_5_tf: Optional[int] = None
    fav_nome_5_tf: Optional[str] = None
    fav_prev_5_tf: Optional[float] = None
    
    # Dados dos Favoritos (Greyhound)
    fav_faixa_1_gh: Optional[int] = None
    fav_nome_1_gh: Optional[str] = None
    fav_faixa_2_gh: Optional[int] = None
    fav_nome_2_gh: Optional[str] = None
    fav_faixa_3_gh: Optional[int] = None
    fav_nome_3_gh: Optional[str] = None

    # Campos para SALVAR o ID no DB
    fav_galgo_id_1_gh: Optional[int] = None
    fav_galgo_id_2_gh: Optional[int] = None
    fav_galgo_id_3_gh: Optional[int] = None
    fav_galgo_id_1_tf: Optional[int] = None
    fav_galgo_id_2_tf: Optional[int] = None
    fav_galgo_id_3_tf: Optional[int] = None
    fav_galgo_id_4_tf: Optional[int] = None
    fav_galgo_id_5_tf: Optional[int] = None

class CorridaCompleta(CorridaBase):
    participantes: List[ParticipanteBase] = []