from sqlalchemy import Column, Integer, String, Numeric, Boolean, Date, Text, ForeignKey, UniqueConstraint, DateTime, Table
from sqlalchemy.orm import relationship
from .conexao import Base
from datetime import datetime, UTC


corrida_historico_associacao = Table('corrida_historico_associacao', Base.metadata,
    Column('corrida_id', Integer, ForeignKey('corridas.id'), primary_key=True),
    Column('historico_corrida_id', Integer, ForeignKey('historico_corridas.id'), primary_key=True)
)

class Pista(Base):
    __tablename__ = 'pistas'
    id = Column(Integer, primary_key=True)
    nome = Column(String(100), unique=True, nullable=False)
    
    # Relacionamento
    corridas = relationship("Corrida", back_populates="pista")
    historico = relationship("HistoricoCorrida", back_populates="pista")

    def __repr__(self):
        return f"<Pista(nome='{self.nome}')>"

class Corrida(Base):
    __tablename__ = 'corridas'
    id = Column(Integer, primary_key=True)
    pista_id = Column(Integer, ForeignKey('pistas.id'), nullable=False)
    
    # Identificadores da corrida
    horario = Column(String(10), nullable=False)
    data_corrida = Column(Date, nullable=False)
    numero_corrida = Column(String(10))
    categoria = Column(String(20))
    distancia = Column(Integer)
    premios = Column(String(100))
    perfil_pista = Column(Text)
    favoritos_tf = Column(Text)
    favoritos_gh = Column(String(20))
    cartao_corrida = Column(Text)
    href_tf = Column(String(500), unique=True)
    href_gh = Column(String(500), unique=True)
    
    # Relacionamentos
    pista = relationship("Pista", back_populates="corridas")
    participacoes = relationship("Participacao", back_populates="corrida", cascade="all, delete-orphan")
    historico_snapshot = relationship("HistoricoCorrida",secondary=corrida_historico_associacao,back_populates="corridas_referencia")

    __table_args__ = (UniqueConstraint('pista_id', 'data_corrida', 'horario', 'numero_corrida', name='uq_corrida_unica'),)

    def __repr__(self):
        return f"<Corrida(pista='{self.pista.nome}', horario='{self.horario}')>"

class Galgo(Base):
    __tablename__ = 'galgos'
    id = Column(Integer, primary_key=True)
    nome = Column(String(150), nullable=False)
    
    # Dados do Galgo
    cor = Column(String(50))
    genero = Column(String(10))
    data_nascimento = Column(Date)
    sire = Column(String(150)) # Pai
    dam = Column(String(150)) # Mãe
    
    # Relacionamentos
    participacoes = relationship("Participacao", back_populates="galgo")
    historico = relationship("HistoricoCorrida", back_populates="galgo", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint('nome', 'data_nascimento', 'sire', 'dam', name='uq_galgo_unico'),)

    def __repr__(self):
        return f"<Galgo(nome='{self.nome}')>"

class Treinador(Base):
    __tablename__ = 'treinadores'
    id = Column(Integer, primary_key=True)
    nome = Column(String(150), unique=True, nullable=False)

    # Relacionamentos
    participacoes = relationship("Participacao", back_populates="treinador")

    def __repr__(self):
        return f"<Treinador(nome='{self.nome}')>"

class Participacao(Base):
    __tablename__ = 'participacoes'
    id = Column(Integer, primary_key=True)
    corrida_id = Column(Integer, ForeignKey('corridas.id'), nullable=False)
    galgo_id = Column(Integer, ForeignKey('galgos.id'), nullable=False)
    treinador_id = Column(Integer, ForeignKey('treinadores.id'))
    
    # Dados da participação
    faixa = Column(Integer)
    win_rec = Column(String(20))
    trap_rec = Column(String(20))
    mstr = Column(Integer)
    sect = Column(Integer)
    posicoes_anteriores = Column(String(50))
    strike_rate = Column(String(10))
    seed = Column(String(20))
    comentario_tf = Column(Text)
    comentario_gh = Column(Text)
    sp_forecast = Column(String(20))
    top_speed = Column(String(20))
    brt = Column(String(20))
    brt_date = Column(Date)
    
    # Relacionamentos
    corrida = relationship("Corrida", back_populates="participacoes")
    galgo = relationship("Galgo", back_populates="participacoes")
    treinador = relationship("Treinador", back_populates="participacoes")

    __table_args__ = (UniqueConstraint('corrida_id', 'galgo_id', name='uq_participacao_unica'),)
    
    def __repr__(self):
        return f"<Participacao(corrida_id={self.corrida_id}, galgo_id={self.galgo_id}, faixa={self.faixa})>"

class HistoricoCorrida(Base):
    __tablename__ = 'historico_corridas'
    id = Column(Integer, primary_key=True)
    galgo_id = Column(Integer, ForeignKey('galgos.id'), nullable=False)
    
    # Dados da corrida histórica
    data = Column(Date)
    tipo_corrida = Column(String(10))
    pista_id = Column(Integer, ForeignKey('pistas.id'))
    distancia = Column(Integer)
    categoria = Column(String(20))
    eye = Column(String(10))
    proxy = Column(String(10))
    faixa = Column(String(10))
    tf_sec = Column(Numeric(5, 2))
    bend = Column(Integer)
    fin = Column(Integer)
    tf_going = Column(String(20))
    isp = Column(String(20))
    tf_time = Column(Numeric(5, 2))
    sec_rtg = Column(String(10))
    rtg = Column(String(10))
    observacoes_tf = Column(Text)
    observacoes_gh = Column(Text)
    split = Column(Numeric(5, 2))
    btn_by = Column(String(15))
    win_sec = Column(String(20))
    wntm = Column(Numeric(5, 2))
    gng = Column(String(20))
    wght = Column(String(20))
    caltm = Column(Numeric(5, 2))
    video_href_raw = Column(String(500), nullable=True)
    video_url = Column(String(500), nullable=True)
    video_status = Column(String(20), nullable=False, default='pending', index=True)
    # Status possíveis: 'pending', 'resolved', 'not_found', 'error'
    
    # Relacionamento
    galgo = relationship("Galgo", back_populates="historico")
    pista = relationship("Pista", back_populates="historico")
    corridas_referencia = relationship("Corrida",secondary=corrida_historico_associacao,back_populates="historico_snapshot")

    __table_args__ = (UniqueConstraint('galgo_id', 'data', 'pista_id', 'distancia', name='uq_historico_unico'),)

    def __repr__(self):
        nome_pista = self.pista.nome if self.pista else 'N/A'
        return f"<HistoricoCorrida(galgo_id={self.galgo_id}, data='{self.data}', pista='{nome_pista}')>"