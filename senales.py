# dao/senales.py
from db_connection import conectar_db
import logging
from decimal import Decimal


def obtener_senales(timestamp):
    """
    Obtiene señales para un timestamp específico.
    Convierte todos los Decimal a float.
    """
    query = """
        SELECT 
            id_senal, 
            id_estrategia_fk, 
            ticker_fk, 
            timestamp_senal,
            tipo_senal, 
            precio_senal, 
            target_profit_price, 
            stop_loss_price, 
            apalancamiento_calculado
        FROM senales_generadas 
        WHERE timestamp_senal = %s
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (timestamp,))
                rows = cur.fetchall()
                if not rows:
                    return []
                columnas = [
                    'id_senal', 'id_estrategia_fk', 'ticker_fk', 'timestamp_senal',
                    'tipo_senal', 'precio_senal', 'target_profit_price', 
                    'stop_loss_price', 'apalancamiento_calculado'
                ]
                # Convertir Decimal a float
                registros = []
                for row in rows:
                    registro = {}
                    for col, val in zip(columnas, row):
                        if isinstance(val, Decimal):
                            registro[col] = float(val)
                        else:
                            registro[col] = val
                    registros.append(registro)
                return registros
    except Exception as e:
        logging.error(f"❌ Error al obtener señales: {e}")
        return []