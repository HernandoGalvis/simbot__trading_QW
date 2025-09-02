# dao/estrategias.py
from db_connection import conectar_db
import logging
from decimal import Decimal

def obtener_parametros_estrategia(id_estrategia):
    """
    Obtiene los parámetros de cierre de una estrategia desde la base de datos.
    """
    query = """
        SELECT 
            porc_limite_retro_entrada,
            porc_limite_retro,
            porc_retroceso_liquidacion_sl,
            porc_liquidacion_parcial_sl
        FROM estrategias 
        WHERE id_estrategia = %s AND activa = true
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (id_estrategia,))
                row = cur.fetchone()
                if not row:
                    error_msg = f"❌ ERROR CRÍTICO: No se encontró estrategia activa con ID {id_estrategia}"
                    logging.error(error_msg)
                    raise ValueError(error_msg)
                
                # Validar que todos los campos requeridos existan
                if row[0] is None or row[1] is None or row[2] is None or row[3] is None:
                    error_msg = f"❌ ERROR CRÍTICO: Parámetros incompletos para estrategia ID {id_estrategia}"
                    logging.error(error_msg)
                    raise ValueError(error_msg)
                
                # Convertir Decimal a float y dividir por 100 para porcentajes
                parametros = {
                    'porc_limite_retro_entrada': float(row[0]) / 100,
                    'porc_limite_retro': float(row[1]) / 100,
                    'porc_retroceso_liquidacion_sl': float(row[2]) / 100,
                    'porc_liquidacion_parcial_sl': float(row[3])
                }
                
                logging.info(f"✅ Parámetros cargados para estrategia {id_estrategia}: {parametros}")
                return parametros
                
    except Exception as e:
        error_msg = f"❌ ERROR CRÍTICO al obtener parámetros de estrategia {id_estrategia}: {e}"
        logging.error(error_msg)
        raise Exception(error_msg)