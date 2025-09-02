# dao/logs.py
from db_connection import conectar_db
from psycopg2.extras import execute_batch
import logging

def guardar_lote_logsxx(eventos):
    """
    Inserta un lote de eventos en log_operaciones_simuladas.
    """
    if not eventos:
        logging.debug("🟡 No hay eventos para guardar en BD.")
        return

    primer_evento = eventos[0]
    logging.info(f"📤 Guardando {len(eventos)} eventos en log_operaciones_simuladas")
    logging.debug(f"📋 Ejemplo de evento: { {k: v for k, v in primer_evento.items() if v is not None} }")

    query = """
        INSERT INTO log_operaciones_simuladas (
            timestamp_evento, id_inversionista_fk, id_senal_fk, id_operacion_fk,
            ticker, tipo_evento, detalle, capital_antes, capital_despues,
            precio_senal, sl, tp, cantidad, motivo_no_operacion,
            motivo_cierre, precio_cierre, resultado, id_estrategia_fk,
            duracion_operacion, porc_sl, porc_tp, volumen_osc_asociado,
            hh_open, hh_close, id_vela_1m_cierre, precio_max_alcanzado,
            precio_min_alcanzado, nro_operacion, fch_registro,
            id_operacion_padre, capital_total_inversionista,
            capital_disponible_inversionista, yyyy_open, mm_open, dd_open,
            yyyy_close, mm_close, dd_close
        ) VALUES (
            %(timestamp_evento)s, %(id_inversionista_fk)s, %(id_senal_fk)s, %(id_operacion_fk)s,
            %(ticker)s, %(tipo_evento)s, %(detalle)s, %(capital_antes)s, %(capital_despues)s,
            %(precio_senal)s, %(sl)s, %(tp)s, %(cantidad)s, %(motivo_no_operacion)s,
            %(motivo_cierre)s, %(precio_cierre)s, %(resultado)s, %(id_estrategia_fk)s,
            %(duracion_operacion)s, %(porc_sl)s, %(porc_tp)s, %(volumen_osc_asociado)s,
            %(hh_open)s, %(hh_close)s, %(id_vela_1m_cierre)s, %(precio_max_alcanzado)s,
            %(precio_min_alcanzado)s, %(nro_operacion)s, %(fch_registro)s,
            %(id_operacion_padre)s, %(capital_total_inversionista)s,
            %(capital_disponible_inversionista)s, %(yyyy_open)s, %(mm_open)s, %(dd_open)s,
            %(yyyy_close)s, %(mm_close)s, %(dd_close)s
        );
    """

    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, eventos)
            conn.commit()
            logging.info(f"✅ {len(eventos)} eventos guardados exitosamente.")
    except Exception as e:
        logging.error(f"❌ ERROR al insertar en log_operaciones_simuladas: {e}")
        logging.error(f"🧾 Detalle del primer evento: {primer_evento}")
        if 'conn' in locals():
            conn.rollback()


def actualizar_capital_inversionista(id_inversionista, capital_actual):
    """
    Actualiza el campo capital_actual en la tabla inversionistas.
    """
    query = "UPDATE inversionistas SET capital_actual = %s WHERE id_inversionista = %s;"
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (capital_actual, id_inversionista))
            conn.commit()
            logging.info(f"✅ Capital del inversionista {id_inversionista} actualizado a {capital_actual}")
    except Exception as e:
        logging.error(f"❌ Error al actualizar capital: {e}")
        if 'conn' in locals():
            conn.rollback()