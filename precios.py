# dao/precios.py
from db_connection import conectar_db
import logging
from decimal import Decimal


def obtener_datos_vela_1m(ticker, timestamp):
    """
    Obtiene id, high, low, close de una vela de 1 minuto.
    Devuelve: (id_vela, high, low, close)
    """
    query = "SELECT id, high, low, close FROM ohlcv_raw_1m WHERE ticker = %s AND \"timestamp\" = %s;"
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ticker, timestamp))
                row = cur.fetchone()
                if not row:
                    return None, None, None, None
                id_vela, high, low, close = row
                return id_vela, float(high), float(low), float(close)
    except Exception as e:
        logging.error(f"❌ Error al obtener datos de vela 1m: {e}")
        return None, None, None, None