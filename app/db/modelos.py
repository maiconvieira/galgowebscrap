from sqlalchemy import (
    Column, Integer, String, Date, Time, Numeric, ForeignKey, 
    UniqueConstraint, Index, Text,
    DateTime, func
)
from sqlalchemy.orm import relationship
from app.db.conexao import Base

class Pista(Base):
    __tablename__ = "pistas"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(100), unique=True, nullable=False, index=True)

class Treinador(Base):
    __tablename__ = "treinadores"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(255), unique=True, nullable=False, index=True)

class Galgo(Base):
    __tablename__ = "galgos"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(255), nullable=False, index=True)
    dt_nasc = Column(Date)
    sexo = Column(String(1))
    cor = Column(String(50))
    sire = Column(String(255)) # Pai
    dam = Column(String(255))  # Mãe
    
    __table_args__ = (
        UniqueConstraint('nome', 'dt_nasc', name='_nome_dt_nasc_uc'),
    )

class Corrida(Base):
    __tablename__ = "corridas"
    id = Column(Integer, primary_key=True, index=True)
    pista_id = Column(Integer, ForeignKey("pistas.id"), nullable=False, index=True)
    href_tf = Column(String(512), unique=True, nullable=False, index=True)
    href_gh = Column(String(512), index=True)
    data_corrida = Column(Date, index=True)
    horario = Column(Time)
    categoria = Column(String(100))
    corrida = Column(Integer)
    distancia = Column(Integer)
    tipo_corrida = Column(String(100))
    premios = Column(String(100))
    perfil_pista = Column(Text)
    cartao_corrida = Column(Text)
    fav_faixa_1_tf = Column(Integer)
    fav_galgo_id_1_tf = Column(Integer, ForeignKey("galgos.id"), nullable=True, index=True)
    fav_prev_1_tf = Column(Numeric(6, 2))
    fav_faixa_2_tf = Column(Integer)
    fav_galgo_id_2_tf = Column(Integer, ForeignKey("galgos.id"), nullable=True, index=True)
    fav_prev_2_tf = Column(Numeric(6, 2))
    fav_faixa_3_tf = Column(Integer)
    fav_galgo_id_3_tf = Column(Integer, ForeignKey("galgos.id"), nullable=True, index=True)
    fav_prev_3_tf = Column(Numeric(6, 2))
    fav_faixa_4_tf = Column(Integer)
    fav_galgo_id_4_tf = Column(Integer, ForeignKey("galgos.id"), nullable=True, index=True)
    fav_prev_4_tf = Column(Numeric(6, 2))
    fav_faixa_5_tf = Column(Integer)
    fav_galgo_id_5_tf = Column(Integer, ForeignKey("galgos.id"), nullable=True, index=True)
    fav_prev_5_tf = Column(Numeric(6, 2))
    fav_faixa_1_gh = Column(Integer)
    fav_galgo_id_1_gh = Column(Integer, ForeignKey("galgos.id"), nullable=True, index=True)
    fav_faixa_2_gh = Column(Integer)
    fav_galgo_id_2_gh = Column(Integer, ForeignKey("galgos.id"), nullable=True, index=True)
    fav_faixa_3_gh = Column(Integer)
    fav_galgo_id_3_gh = Column(Integer, ForeignKey("galgos.id"), nullable=True, index=True)

    # Relacionamentos
    pista = relationship("Pista")
    participantes = relationship("Participante", back_populates="corrida", cascade="all, delete-orphan")
    fav_galgo_1_gh_rel = relationship("Galgo", foreign_keys=[fav_galgo_id_1_gh])
    fav_galgo_2_gh_rel = relationship("Galgo", foreign_keys=[fav_galgo_id_2_gh])
    fav_galgo_3_gh_rel = relationship("Galgo", foreign_keys=[fav_galgo_id_3_gh])
    fav_galgo_1_tf_rel = relationship("Galgo", foreign_keys=[fav_galgo_id_1_tf])
    fav_galgo_2_tf_rel = relationship("Galgo", foreign_keys=[fav_galgo_id_2_tf])
    fav_galgo_3_tf_rel = relationship("Galgo", foreign_keys=[fav_galgo_id_3_tf])
    fav_galgo_4_tf_rel = relationship("Galgo", foreign_keys=[fav_galgo_id_4_tf])
    fav_galgo_5_tf_rel = relationship("Galgo", foreign_keys=[fav_galgo_id_5_tf])

class Participante(Base):
    __tablename__ = "participantes"
    id = Column(Integer, primary_key=True, index=True)
    corrida_id = Column(Integer, ForeignKey("corridas.id", ondelete="CASCADE"), nullable=False, index=True)
    galgo_id = Column(Integer, ForeignKey("galgos.id"), nullable=False, index=True)
    treinador_id = Column(Integer, ForeignKey("treinadores.id"), index=True)
    faixa = Column(Integer, nullable=False)
    form = Column(String(100)) # Ex: "12345"
    strike_rate = Column(Numeric(5, 2)) # % de vitória do treinador
    mstr = Column(Integer)
    sect = Column(Integer)
    seed = Column(String(50))
    win_rec = Column(Numeric(5, 2))  # Convertido de "1/5"
    trap_rec = Column(Numeric(5, 2)) # Convertido de "0/3"
    sp_forecast = Column(Numeric(6, 2)) # Convertido de "5/1"
    topspeed = Column(Integer)
    brt = Column(Numeric(5, 2))
    categoria_brt = Column(String(50))
    data_brt = Column(Date)
    comentario_tf = Column(Text)
    comentario_gh = Column(Text)
    
    # Features Agregadas (permanecem)
    hist_avg_pos_ult_5 = Column(Numeric(4, 2))
    hist_win_rate_ult_5 = Column(Numeric(3, 2))

    # Relacionamentos
    corrida = relationship("Corrida", back_populates="participantes")
    galgo = relationship("Galgo")
    treinador = relationship("Treinador")
    historico = relationship("HistoricoCorrida", back_populates="participante", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('corrida_id', 'faixa', name='_corrida_faixa_uc'),
    )

class HistoricoCorrida(Base):
    __tablename__ = "historico_corridas"
    id = Column(Integer, primary_key=True, index=True)
    participante_id = Column(Integer, ForeignKey("participantes.id", ondelete="CASCADE"), nullable=False, index=True)
    pista_id = Column(Integer, ForeignKey("pistas.id"), index=True)
    data = Column(Date, index=True)
    distancia = Column(Integer)
    categoria = Column(String(100))
    tipo_corrida = Column(String(100))
    faixa = Column(Integer)
    fin = Column(Integer)      # Posição final (ex: 1, 2, 3, ou None para 'DN')
    btn_tf = Column(String(50))
    btn_gh = Column(String(50))
    split_tf = Column(Numeric(5, 2))
    split_gh = Column(Numeric(5, 2))
    time_tf = Column(Numeric(5, 2))
    time_gh = Column(Numeric(5, 2))
    time_win = Column(Numeric(5, 2)) # Tempo do vencedor ('wntm')
    proxy = Column(Integer)
    bend = Column(String(50))
    going_tf = Column(String(50))
    going_gh = Column(String(50))
    sec_rtg = Column(Integer)
    rtg = Column(Integer)
    sp_odds = Column(Numeric(5, 2))
    sp_fav = Column(Integer, nullable=True)
    peso = Column(Numeric(4, 1)) # 'wght'
    video_src = Column(String(1024))
    pri_ou_seg = Column(String(255)) # 'win_sec'
    observacoes_tf = Column(Text)
    observacoes_gh = Column(Text)

    # Relacionamentos
    participante = relationship("Participante", back_populates="historico")
    pista = relationship("Pista")

class BackfillLog(Base):
    __tablename__ = 'backfill_log'
    data_corrida = Column(Date, primary_key=True)
    status = Column(String(50), nullable=False, default='pending') # pending, running, success, failed, no_races
    last_updated = Column(DateTime(timezone=True), onupdate=func.now(), default=func.now())