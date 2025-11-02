# _temp_reset_db.py
import logging
from app.processing.processador import criar_tabelas
from app.db.conexao import engine, Base
# Precisamos importar TODOS os modelos para o 'Base' os conhecer
from app.db import modelos 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    logging.warning("### INICIANDO RECRIAÇÃO MANUAL DO SCHEMA ###")
    # Isso vai rodar o DROP SCHEMA ... CASCADE;
    # e depois o Base.metadata.create_all()
    criar_tabelas() 
    logging.info("### SCHEMA RECRIADO COM SUCESSO ###")
    logging.info("O banco de dados agora está 100% sincronizado com 'modelos.py'.")
except Exception as e:
    logging.critical("Falha ao recriar o schema.", exc_info=True)