# dao/precios.py
from db_connection import conectar_db
import logging
from decimal import Decimal

"""
Acceso a velas de 1 minuto.

Tabla esperada: ohlcv_raw_1m(ticker, timestamp, id, high, low, close)

Funciones públicas:
- obtener_datos_vela_1m(ticker, ts) -> (id, high, low, close) (compatibilidad)
- obtener_precio_min_max_close(ticker, ts) -> (high, low, close)
- obtener_id_vela_1m(ticker, ts) -> id
- obtener_close_1m(ticker, ts) -> close
"""

def _obtener_crudo_vela_1m(ticker: str, timestamp):
    query = """
        SELECT id, high, low, close
        FROM ohlcv_raw_1m
        WHERE ticker = %s AND "timestamp" = %s
        LIMIT 1;
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ticker, timestamp))
                row = cur.fetchone()
                if not row:
                    return None, None, None, None
                id_vela, high, low, close = row

                def to_float(v):
                    if isinstance(v, (Decimal,)):
                        return float(v)
                    return float(v) if v is not None else None

                return id_vela, to_float(high), to_float(low), to_float(close)
    except Exception as e:
        logging.error(f"❌ Error al obtener vela 1m: {e}")
        return None, None, None, None


# --- Función original mantenida por compatibilidad ---
def obtener_datos_vela_1m(ticker, timestamp):
    return _obtener_crudo_vela_1m(ticker, timestamp)


def obtener_precio_min_max_close(ticker, timestamp):
    _, high, low, close = _obtener_crudo_vela_1m(ticker, timestamp)
    return high, low, close


def obtener_id_vela_1m(ticker, timestamp):
    id_vela, _, _, _ = _obtener_crudo_vela_1m(ticker, timestamp)
    return id_vela


def obtener_close_1m(ticker, timestamp):
    _, _, _, close = _obtener_crudo_vela_1m(ticker, timestamp)
    return close
