# dao/inversionistas.py
from db_connection import conectar_db
import logging
from decimal import Decimal


def obtener_todos_inversionistas_activos():
    """
    Obtiene todos los inversionistas activos desde la base de datos.
    """
    query = """
        SELECT 
            id_inversionista,
            capital_aportado,
            riesgo_max_operacion_pct AS riesgo_max_pct,
            tamano_min_operacion AS tamano_min,
            tamano_max_operacion AS tamano_max,
            limite_diario_operaciones AS limite_diario,
            limite_operaciones_abiertas AS limite_abiertas,
            apalancamiento_max,
            comision_operacion_pct AS comision_pct,
            slippage_pct,
            usar_parametros_senal
        FROM inversionistas 
        WHERE activo = true
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                if not rows:
                    return []
                columnas = [
                    'id_inversionista', 'capital_aportado',
                    'riesgo_max_pct', 'tamano_min', 'tamano_max',
                    'limite_diario', 'limite_abiertas',
                    'apalancamiento_max', 'comision_pct',
                    'slippage_pct', 'usar_parametros_senal'
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
        logging.error(f"❌ Error al obtener inversionistas activos: {e}")
        return []