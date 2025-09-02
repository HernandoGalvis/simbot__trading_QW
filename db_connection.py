# db_connection.py
import psycopg2
from parmspg import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
import logging

# Conexión global (única)
_conn = None

def conectar_db():
    """
    Retorna una conexión a PostgreSQL (una sola vez).
    """
    global _conn
    if _conn is None:
        try:
            _conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                connect_timeout=10
            )
            logging.info("✅ Conexión a PostgreSQL establecida.")
        except Exception as e:
            logging.error(f"❌ No se pudo conectar a la base de datos: {e}")
            raise
    return _conn

def cerrar_db():
    """
    Cierra la conexión global.
    """
    global _conn
    if _conn:
        _conn.close()
        _conn = None
        logging.info("🔌 Conexión a PostgreSQL cerrada.")