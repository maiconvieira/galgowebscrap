from sqlalchemy import text
from conexao import engine, Base, testar_conexao
from modelos import *

def dropar_tabelas():
    try:
        Base.metadata.drop_all(engine)
        return True
    except Exception as e:
        print(f"Erro ao remover tabelas: {e}")
        return False

if __name__ == "__main__":
    dropar_tabelas()