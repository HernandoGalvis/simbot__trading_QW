# dao/operaciones.py
from db_connection import conectar_db
import logging


def crear_operacion_en_bd(
    id_senal, ticker, tipo_operacion, precio_entrada, cantidad,
    apalancamiento, stop_loss, take_profit, id_operacion_padre,
    id_inversionista_fk, id_estrategia_fk, timestamp_apertura,
    capital_riesgo_usado, valor_total_exposicion, porc_sl, porc_tp,
    precio_max_alcanzado, cnt_operaciones, id_vela_1m_apertura=None
):
    query = """
        INSERT INTO operaciones_simuladas (
            id_inversionista_fk, id_estrategia_fk, id_senal_fk, ticker_fk,
            tipo_operacion, precio_entrada, cantidad, apalancamiento,
            stop_loss_price, take_profit_price, id_operacion_padre,
            timestamp_apertura, capital_riesgo_usado, valor_total_exposicion,
            porc_sl, porc_tp, precio_max_alcanzado, cnt_operaciones,
            id_vela_1m_apertura
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id_operacion;
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (
                    id_inversionista_fk, id_estrategia_fk, id_senal, ticker,
                    tipo_operacion, precio_entrada, cantidad, apalancamiento,
                    stop_loss, take_profit, id_operacion_padre,
                    timestamp_apertura, capital_riesgo_usado, valor_total_exposicion,
                    porc_sl, porc_tp, precio_max_alcanzado, cnt_operaciones,
                    id_vela_1m_apertura  # ✅ Agregar ID de vela de apertura
                ))
                id_operacion = cur.fetchone()[0]
                conn.commit()
                logging.info(f"✅ Operación creada en BD: ID={id_operacion} | {ticker} | {tipo_operacion} | Vela ID={id_vela_1m_apertura}")
                return id_operacion
    except Exception as e:
        logging.error(f"❌ Error al crear operación en BD: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise


def actualizar_operacion_dca(
    id_operacion, precio_entrada, cantidad, capital_riesgo_usado,
    valor_total_exposicion, cnt_operaciones
):
    """
    Actualiza operación tras DCA.
    """
    query = """
        UPDATE operaciones_simuladas SET
            precio_entrada = %s,
            cantidad = %s,
            capital_riesgo_usado = %s,
            valor_total_exposicion = %s,
            cnt_operaciones = %s
        WHERE id_operacion = %s;
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (
                    precio_entrada, cantidad, capital_riesgo_usado,
                    valor_total_exposicion, cnt_operaciones, id_operacion
                ))
            conn.commit()
            # ✅ Corregido: usar id_operacion, no id_op
            logging.info(f"🔁 DCA actualizado en BD: ID={id_operacion}")
    except Exception as e:
        logging.error(f"❌ Error al actualizar DCA: {e}")


def actualizar_operacion_cierre(
    id_operacion, timestamp_cierre, precio_cierre, resultado,
    motivo_cierre, duracion_operacion, id_vela_1m_cierre
):
    """
    Actualiza todos los campos al cerrar una operación.
    """
    query = """
        UPDATE operaciones_simuladas SET
            timestamp_cierre = %s,
            precio_cierre = %s,
            resultado = %s,
            motivo_cierre = %s,
            duracion_operacion = %s,
            id_vela_1m_cierre = %s,
            estado = 'cerrada_total'
        WHERE id_operacion = %s;
    """
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (
                    timestamp_cierre, precio_cierre, resultado,
                    motivo_cierre, duracion_operacion, id_vela_1m_cierre,
                    id_operacion
                ))
            conn.commit()
            logging.info(f"CloseOperation actualizado en BD: ID={id_operacion}")
    except Exception as e:
        logging.error(f"❌ Error al actualizar cierre: {e}")


def actualizar_precio_max_min(id_operacion, precio_max, precio_min):
    """
    Actualiza los precios máximos y mínimos alcanzados.
    """
    query = "UPDATE operaciones_simuladas SET precio_max_alcanzado = %s, precio_min_alcanzado = %s WHERE id_operacion = %s;"
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (precio_max, precio_min, id_operacion))
            conn.commit()
    except Exception as e:
        logging.error(f"❌ Error al actualizar precios extremos: {e}")


def obtener_id_vela_1m(ticker, timestamp):
    """
    Obtiene el id de la vela de 1 minuto.
    """
    query = "SELECT id FROM ohlcv_raw_1m WHERE ticker = %s AND \"timestamp\" = %s;"
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ticker, timestamp))
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        logging.error(f"❌ Error al obtener id_vela_1m: {e}")
        return None


def actualizar_pyg_no_realizado(id_operacion, pyg_no_realizado):
    """
    Actualiza el pyg_no_realizado en BD.
    """
    query = "UPDATE operaciones_simuladas SET pyg_no_realizado = %s WHERE id_operacion = %s;"
    try:
        with conectar_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (pyg_no_realizado, id_operacion))
            conn.commit()
    except Exception as e:
        logging.error(f"❌ Error al actualizar pyg_no_realizado: {e}")