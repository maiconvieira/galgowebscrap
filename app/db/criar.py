from sqlalchemy import text
from conexao import engine, Base, testar_conexao
from modelos import *

def criar_tabelas():
    try:
        if not testar_conexao():
            return False

        Base.metadata.create_all(engine)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tabelas = [row[0] for row in result]
            #print(f"Tabelas existentes: {', '.join(tabelas)}")
        
        return True
        
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")
        return False

if __name__ == "__main__":
    criar_tabelas()