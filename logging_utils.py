# modulos/logging_utils.py
from db_connection import conectar_db
import logging
from datetime import datetime
from psycopg2.extras import execute_batch


def registrar_evento(
    inversionista,
    tipo_evento,
    id_operacion_fk=None,
    id_senal_fk=None,
    ticker=None,
    tipo_operacion=None,
    cantidad=None,
    precio_entrada=None,
    precio_cierre=None,
    resultado=None,
    motivo_cierre=None,
    motivo_no_operacion=None,
    capital_antes=None,
    capital_despues=None,
    detalle=None,
    id_operacion_padre=None,
    id_estrategia_fk=None,
    duracion_operacion=None,
    porc_sl=None,
    porc_tp=None,
    volumen_osc_asociado=None,
    id_vela_1m_cierre=None,
    precio_max_alcanzado=None,
    precio_min_alcanzado=None,
    nro_operacion=None,
    precio_senal=None,
    sl=None,
    tp=None,
    timestamp_evento=None,  # ✅ Nuevo parámetro
    id_vela_1m_apertura=None  # ✅ Nuevo parámetro para ID de vela de apertura
):
    """
    Registra un evento en log_operaciones_simuladas con todos los campos disponibles.
    """
    # ✅ Usar timestamp_evento de la señal, no utcnow()
    if not timestamp_evento:
        timestamp_evento = datetime.utcnow()

    query = """
        INSERT INTO log_operaciones_simuladas (
            timestamp_evento,
            id_inversionista_fk,
            id_senal_fk,
            id_operacion_fk,
            ticker,
            tipo_evento,
            detalle,
            capital_antes,
            capital_despues,
            precio_senal,
            sl,
            tp,
            cantidad,
            motivo_no_operacion,
            resultado,
            motivo_cierre,
            precio_cierre,
            id_estrategia_fk,
            duracion_operacion,
            porc_sl,
            porc_tp,
            volumen_osc_asociado,
            id_vela_1m_cierre,
            precio_max_alcanzado,
            precio_min_alcanzado,
            nro_operacion,
            id_vela_1m_apertura
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (
                    timestamp_evento,
                    inversionista.id,
                    id_senal_fk,
                    id_operacion_fk,
                    ticker,
                    tipo_evento,
                    detalle,
                    capital_antes,
                    capital_despues,
                    precio_senal,
                    sl,
                    tp,
                    cantidad,
                    motivo_no_operacion,
                    resultado,
                    motivo_cierre,
                    precio_cierre,
                    id_estrategia_fk,
                    duracion_operacion,
                    porc_sl,
                    porc_tp,
                    volumen_osc_asociado,
                    id_vela_1m_cierre,
                    precio_max_alcanzado,
                    precio_min_alcanzado,
                    nro_operacion,
                    id_vela_1m_apertura  # ✅ Agregar ID de vela de apertura
                ))
            conn.commit()
    except Exception as e:
        logging.error(f"❌ Error al registrar evento en log: {e}")


def vaciar_log_a_bd(inversionista):
    """
    Vacía los eventos en memoria al log de la base de datos.
    """
    if not inversionista.log_eventos:
        logging.debug("🟡 No hay eventos para guardar en BD.")
        return

    query = """
        INSERT INTO log_operaciones_simuladas (
            timestamp_evento,
            id_inversionista_fk,
            id_senal_fk,
            id_operacion_fk,
            ticker,
            tipo_evento,
            detalle,
            capital_antes,
            capital_despues,
            precio_senal,
            sl,
            tp,
            cantidad,
            motivo_no_operacion,
            resultado,
            motivo_cierre,
            precio_cierre,
            id_estrategia_fk,
            duracion_operacion,
            porc_sl,
            porc_tp,
            volumen_osc_asociado,
            id_vela_1m_cierre,
            precio_max_alcanzado,
            precio_min_alcanzado,
            nro_operacion,
            id_vela_1m_apertura
        ) VALUES (
            %(timestamp_evento)s, %(id_inversionista_fk)s, %(id_senal_fk)s, %(id_operacion_fk)s,
            %(ticker)s, %(tipo_evento)s, %(detalle)s, %(capital_antes)s, %(capital_despues)s,
            %(precio_senal)s, %(sl)s, %(tp)s, %(cantidad)s, %(motivo_no_operacion)s,
            %(resultado)s, %(motivo_cierre)s, %(precio_cierre)s, %(id_estrategia_fk)s,
            %(duracion_operacion)s, %(porc_sl)s, %(porc_tp)s, %(volumen_osc_asociado)s,
            %(id_vela_1m_cierre)s, %(precio_max_alcanzado)s, %(precio_min_alcanzado)s,
            %(nro_operacion)s, %(id_vela_1m_apertura)s
        );
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, inversionista.log_eventos)
            conn.commit()
            logging.info(f"✅ {len(inversionista.log_eventos)} eventos guardados exitosamente.")
    except Exception as e:
        logging.error(f"❌ Error al vaciar log a BD: {e}")
        if 'conn' in locals():
            conn.rollback()