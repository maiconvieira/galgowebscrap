import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

# Certificado de conexão com DB
RDS_CERT_PATH = "ca_sinsej.pem"
load_dotenv()

# Dados da conexão
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

# String de conexão com SSL
connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

# Criando o engine com suporte a SSL
engine = create_engine(
    connection_string,
    connect_args={
        "sslmode": "require", 
        "sslrootcert": RDS_CERT_PATH
    }
)

# Base para as classes ORM
Base = declarative_base()

# Session maker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Função para obter sessão do banco
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Teste de conexão
def testar_conexao():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            db_version = result.scalar()
            
            # Teste adicional para verificar se podemos acessar tabelas
            result_tables = connection.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = [row[0] for row in result_tables]
            
            return True
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        return False

if __name__ == "__main__":
    testar_conexao()