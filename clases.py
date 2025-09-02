# clases.py
from datetime import datetime
from typing import Dict, List, Optional
import logging
from decimal import Decimal
from dao.operaciones import crear_operacion_en_bd, actualizar_operacion_cierre, actualizar_operacion_dca, actualizar_precio_max_min


def calcular_precio_promedio(precio1, cant1, precio2, cant2):
    """
    Calcula el precio promedio ponderado tras un DCA.
    """
    total_valor = precio1 * cant1 + precio2 * cant2
    total_cant = cant1 + cant2
    return total_valor / total_cant if total_cant > 0 else precio1


def aplicar_slippage(precio, slippage_pct, tipo):
    """
    Aplica slippage al precio de entrada.
    Maneja Decimal y float correctamente.
    """
    precio = Decimal(str(precio)) if not isinstance(precio, Decimal) else precio
    slippage_pct = Decimal(str(slippage_pct))
    factor = (Decimal('1') + slippage_pct / Decimal('100')) if tipo == 'LONG' \
             else (Decimal('1') - slippage_pct / Decimal('100'))
    return float(precio * factor)


class Inversionista:
    """
    Representa un inversionista con capital, límites y estado de operaciones.
    """

    def __init__(self, id_inv, capital, config):
        self.id = id_inv
        self.capital_aportado = float(capital)
        self.capital_actual = float(capital)  # ✅ Inicializa con capital aportado
        self.riesgo_max_pct = config['riesgo_max_pct']  # ✅ Agregar el porcentaje de riesgo
        self.riesgo_max_usd = self.capital_aportado * (config['riesgo_max_pct'] / 100)
        self.tamano_min = config['tamano_min']
        self.tamano_max = config['tamano_max']
        self.limite_diario = config['limite_diario']
        self.limite_abiertas = config['limite_abiertas']
        self.apalancamiento_max = config['apalancamiento_max']
        self.comision_pct = config['comision_pct']
        self.slippage_pct = config['slippage_pct']
        
        # ✅ Nuevo: usar_parametros_senal
        self.usar_parametros_senal = config.get('usar_parametros_senal', False)
        
        # Estado
        self.operaciones_hoy = 0
        self.fecha_actual_operaciones = None  # ✅ Nueva: fecha del conteo de operaciones
        self.operaciones_activas: Dict[str, 'Operacion'] = {}  # clave: f"{ticker}-{tipo}"
        self.log_eventos: List[Dict] = []  # Eventos en memoria antes de guardar

        logging.info(f"👤 Inversionista {self.id} cargado | Capital: {self.capital_actual:.2f}")

    def verificar_y_reiniciar_contadores(self, fecha_actual):
        """
        Reinicia contadores diarios si cambiamos de día.
        """
        fecha_operaciones = fecha_actual.date()
        if self.fecha_actual_operaciones != fecha_operaciones:
            self.operaciones_hoy = 0
            self.fecha_actual_operaciones = fecha_operaciones
            logging.info(f"🔄 Contadores diarios reiniciados para inversionista {self.id}")


class Operacion:
    """
    Representa una operación de trading (LONG/SHORT), con soporte para DCA y cierres parciales.
    """

    def __init__(
        self, id_senal, ticker, tipo, precio, cant, apal, sl, tp,
        padre=None, id_inversionista=None, id_estrategia_fk=None,
        timestamp_apertura=None,
        inversionista_obj=None,  # ✅ Nuevo: objeto completo del inversionista
        id_vela_1m_apertura=None  # ✅ Nuevo: ID de vela de apertura
    ):
        self.id_operacion: Optional[int] = None
        self.id_senal = id_senal
        self.ticker = ticker
        self.tipo_operacion = tipo  # "LONG" o "SHORT"
        self.precio_entrada = float(precio)
        self.cantidad = float(cant)
        self.apalancamiento = apal
        self.stop_loss = float(sl) if sl else 0.0
        self.take_profit = float(tp) if tp else 0.0
        self.timestamp_apertura = timestamp_apertura or datetime.utcnow()
        self.timestamp_cierre: Optional[datetime] = None
        self.precio_cierre: Optional[float] = None
        self.resultado = 0.0
        self.motivo_cierre: Optional[str] = None
        self.estado = "abierta"
        self.id_operacion_padre = padre

        # Guardar id_estrategia_fk para usarlo en cierres parciales
        self.id_estrategia_fk = id_estrategia_fk

        # ✅ Guardar ID de vela de apertura
        self.id_vela_1m_apertura = id_vela_1m_apertura

        # Seguimiento de precios extremos
        self.precio_max_alcanzado = self.precio_entrada
        self.precio_min_alcanzado = self.precio_entrada

        # Cálculos nuevos: agregar valores reales
        self.capital_riesgo_usado = self.cantidad * self.precio_entrada
        self.valor_total_exposicion = self.capital_riesgo_usado * self.apalancamiento
        self.porc_sl = abs((self.precio_entrada - self.stop_loss) / self.precio_entrada) * 100
        self.porc_tp = abs((self.take_profit - self.precio_entrada) / self.precio_entrada) * 100

        # Cálculos existentes
        self.duracion_operacion = 0.0
        self.pyg_no_realizado = 0.0
        self.cnt_operaciones = 1  # Apertura = 1

        # ✅ Insertar en BD con todos los campos calculados
        try:
            self.id_operacion = crear_operacion_en_bd(
                id_senal=self.id_senal,
                ticker=self.ticker,
                tipo_operacion=self.tipo_operacion,
                precio_entrada=self.precio_entrada,
                cantidad=self.cantidad,
                apalancamiento=self.apalancamiento,
                stop_loss=self.stop_loss,
                take_profit=self.take_profit,
                id_operacion_padre=self.id_operacion_padre,
                id_inversionista_fk=id_inversionista,
                id_estrategia_fk=id_estrategia_fk,
                timestamp_apertura=self.timestamp_apertura,
                capital_riesgo_usado=self.capital_riesgo_usado,
                valor_total_exposicion=self.valor_total_exposicion,
                porc_sl=self.porc_sl,
                porc_tp=self.porc_tp,
                precio_max_alcanzado=self.precio_max_alcanzado,
                cnt_operaciones=self.cnt_operaciones,
                id_vela_1m_apertura=self.id_vela_1m_apertura  # ✅ Pasar ID de vela de apertura
            )
        except Exception as e:
            logging.error(f"❌ Fallo al crear operación en BD: {e}")
            raise

        # ✅ Registrar evento solo si se pasa el objeto completo del inversionista
        if inversionista_obj is not None:
            from modulos.logging_utils import registrar_evento
            registrar_evento(
                inversionista=inversionista_obj,
                tipo_evento="apertura",
                id_operacion_fk=self.id_operacion,
                id_senal_fk=self.id_senal,
                ticker=self.ticker,
                tipo_operacion=self.tipo_operacion,
                cantidad=self.cantidad,
                precio_entrada=self.precio_entrada,
                capital_antes=inversionista_obj.capital_actual + self.capital_riesgo_usado,
                capital_despues=inversionista_obj.capital_actual,
                detalle=f"Apertura: {self.ticker} | {self.tipo_operacion} | {self.cantidad:.6f} @ {self.precio_entrada}",
                id_estrategia_fk=self.id_estrategia_fk,
                porc_sl=self.porc_sl,
                porc_tp=self.porc_tp,
                precio_senal=precio,
                sl=self.stop_loss,
                tp=self.take_profit,
                nro_operacion=self.cnt_operaciones,
                id_vela_1m_apertura=self.id_vela_1m_apertura  # ✅ Pasar ID de vela de apertura
            )

        logging.info(f"🆕 Operación creada: {self.ticker} | {self.tipo_operacion} | "
                     f"Cantidad={self.cantidad:.6f} | Precio={self.precio_entrada} | "
                     f"SL={self.stop_loss} | TP={self.take_profit} | Vela ID={self.id_vela_1m_apertura}")

    def actualizar_precio(self, precio, timestamp):
        """
        Actualiza el seguimiento de precios extremos.
        Usa el precio de cierre de la vela.
        """
        precio = float(precio)
        if self.tipo_operacion == "LONG":
            if precio > self.precio_max_alcanzado:
                self.precio_max_alcanzado = precio
                actualizar_precio_max_min(self.id_operacion, self.precio_max_alcanzado, self.precio_min_alcanzado)
                logging.debug(f"📈 {self.ticker} | Nuevo máximo alcanzado: {precio}")
        elif self.tipo_operacion == "SHORT":
            if precio < self.precio_min_alcanzado:
                self.precio_min_alcanzado = precio
                actualizar_precio_max_min(self.id_operacion, self.precio_max_alcanzado, self.precio_min_alcanzado)
                logging.debug(f"📉 {self.ticker} | Nuevo mínimo alcanzado: {precio}")

    def aplicar_dca(self, inversionista, precio, cantidad):
        """
        Acumula cantidad y recalcula precio promedio.
        NO registra eventos de logging - eso se hace en el nivel superior.
        """
        logging.info(f"🔁 DCA en {self.ticker}: {cantidad:.6f} @ {precio}")
        nuevo_precio = calcular_precio_promedio(
            self.precio_entrada, self.cantidad,
            precio, cantidad
        )
        self.precio_entrada = nuevo_precio
        self.cantidad += cantidad

        # Recalcular después de DCA
        self.capital_riesgo_usado = self.cantidad * self.precio_entrada
        self.valor_total_exposicion = self.capital_riesgo_usado * self.apalancamiento
        self.cnt_operaciones += 1  # Incrementar contador

        # Actualizar en BD
        actualizar_operacion_dca(
            self.id_operacion,
            self.precio_entrada,
            self.cantidad,
            self.capital_riesgo_usado,
            self.valor_total_exposicion,
            self.cnt_operaciones
        )

        # ✅ NO registrar evento aquí - se hace en simulador.py con datos correctos
        logging.info(f"🔁 DCA aplicado: {self.ticker} | {self.tipo_operacion} | +{cantidad:.6f}")

    def calcular_resultado(self, precio_salida):
        """
        Calcula ganancia o pérdida (sin comisiones).
        """
        if self.tipo_operacion == "LONG":
            return (precio_salida - self.precio_entrada) * self.cantidad
        else:  # SHORT
            return (self.precio_entrada - precio_salida) * self.cantidad

    def cerrar_parcial(self, inversionista, precio_cierre, porc_liquidar):
        """
        Cierra parcialmente y devuelve nueva operación hija.
        """
        logging.warning(f"⚠️  Cierre parcial por SL en {self.ticker}: {porc_liquidar}% del tamaño")

        cantidad_liquidar = self.cantidad * (porc_liquidar / 100)
        cantidad_restante = self.cantidad - cantidad_liquidar
        resultado_parcial = self.calcular_resultado(precio_cierre) * (cantidad_liquidar / self.cantidad)

        # Actualizar operación actual
        self.cantidad = cantidad_restante
        self.valor_total_exposicion = self.cantidad * self.precio_entrada * self.apalancamiento
        self.timestamp_cierre = datetime.utcnow()
        self.precio_cierre = precio_cierre
        self.resultado = resultado_parcial
        self.motivo_cierre = "Liquidación parcial por SL"
        self.estado = "cerrada_parcial"

        from modulos.logging_utils import registrar_evento
        registrar_evento(
            inversionista=inversionista,
            tipo_evento="cierre_parcial",
            id_operacion_fk=self.id_operacion,
            id_senal_fk=self.id_senal,
            ticker=self.ticker,
            cantidad=cantidad_liquidar,
            precio_cierre=precio_cierre,
            resultado=resultado_parcial,
            motivo_cierre="Liquidación parcial por SL",
            capital_antes=inversionista.capital_actual,
            capital_despues=inversionista.capital_actual + resultado_parcial,
            id_operacion_padre=self.id_operacion,
            detalle=f"Cierre parcial por SL | {porc_liquidar}% liquidado | Resultado={resultado_parcial:+.2f}",
            id_estrategia_fk=self.id_estrategia_fk,
            duracion_operacion=self.duracion_operacion,
            porc_sl=self.porc_sl,
            porc_tp=self.porc_tp,
            id_vela_1m_cierre=None,
            precio_max_alcanzado=self.precio_max_alcanzado,
            precio_min_alcanzado=self.precio_min_alcanzado,
            nro_operacion=self.cnt_operaciones,
            sl=self.stop_loss,
            tp=self.take_profit
        )

        # Crear operación hija
        operacion_hija = Operacion(
            id_senal=self.id_senal,
            ticker=self.ticker,
            tipo=self.tipo_operacion,
            precio=self.precio_entrada,
            cant=cantidad_restante,
            apal=self.apalancamiento,
            sl=self.stop_loss,
            tp=self.take_profit,
            padre=self.id_operacion,
            id_inversionista=inversionista.id,
            id_estrategia_fk=self.id_estrategia_fk,
            timestamp_apertura=self.timestamp_apertura,
            inversionista_obj=inversionista,  # ✅ Pasar objeto completo
            id_vela_1m_apertura=self.id_vela_1m_apertura  # ✅ Pasar ID de vela de apertura original
        )
        inversionista.operaciones_activas[f"{self.ticker}-{self.tipo_operacion}"] = operacion_hija

        registrar_evento(
            inversionista=inversionista,
            tipo_evento="apertura_hija",
            id_operacion_fk=operacion_hija.id_operacion,
            ticker=operacion_hija.ticker,
            cantidad=operacion_hija.cantidad,
            precio_entrada=operacion_hija.precio_entrada,
            id_operacion_padre=self.id_operacion,
            detalle=f"Operación hija creada tras cierre parcial | ID={operacion_hija.id_operacion}",
            id_estrategia_fk=operacion_hija.id_estrategia_fk,
            porc_sl=operacion_hija.porc_sl,
            porc_tp=operacion_hija.porc_tp,
            nro_operacion=operacion_hija.cnt_operaciones,
            id_vela_1m_apertura=operacion_hija.id_vela_1m_apertura  # ✅ Pasar ID de vela de apertura
        )

        logging.info(f"👶 Operación hija creada: ID={operacion_hija.id_operacion} | "
                     f"Cantidad={cantidad_restante:.6f} | Precio={operacion_hija.precio_entrada}")

        return operacion_hija

    def cerrar_total(self, inversionista, precio_cierre, motivo, ts_cierre, id_vela_1m_cierre):
        """
        Cierra totalmente la operación.
        """
        self.timestamp_cierre = ts_cierre
        self.precio_cierre = float(precio_cierre)
        self.resultado = self.calcular_resultado(precio_cierre)
        self.motivo_cierre = motivo
        self.estado = "cerrada_total"
        self.duracion_operacion = (self.timestamp_cierre - self.timestamp_apertura).total_seconds() / 60

        # ✅ Devolver capital + ganancia/pérdida = cantidad * precio_cierre
        capital_devuelto = self.cantidad * self.precio_cierre
        inversionista.capital_actual += capital_devuelto

        from modulos.logging_utils import registrar_evento
        registrar_evento(
            inversionista=inversionista,
            tipo_evento="cierre_total",
            id_operacion_fk=self.id_operacion,
            id_senal_fk=self.id_senal,  # ✅ Usar el ID de señal original
            ticker=self.ticker,
            cantidad=self.cantidad,
            precio_cierre=self.precio_cierre,
            resultado=self.resultado,
            motivo_cierre=motivo,
            capital_antes=inversionista.capital_actual - capital_devuelto,
            capital_despues=inversionista.capital_actual,
            detalle=f"Cierre total por {motivo} | Resultado={self.resultado:+.2f} | "
                    f"Precio={self.precio_cierre} | Duración={self.duracion_operacion:.1f} min",
            id_estrategia_fk=self.id_estrategia_fk,
            duracion_operacion=self.duracion_operacion,
            porc_sl=self.porc_sl,
            porc_tp=self.porc_tp,
            id_vela_1m_cierre=id_vela_1m_cierre,
            precio_max_alcanzado=self.precio_max_alcanzado,
            precio_min_alcanzado=self.precio_min_alcanzado,
            nro_operacion=self.cnt_operaciones,
            sl=self.stop_loss,
            tp=self.take_profit,
            precio_senal=self.precio_cierre,  # ✅ Usar el precio de cierre como precio_senal
            timestamp_evento=ts_cierre  # ✅ Usar el timestamp de la vela que causó el cierre
        )

        # Actualizar en BD todos los campos
        actualizar_operacion_cierre(
            self.id_operacion,
            self.timestamp_cierre,
            self.precio_cierre,
            self.resultado,
            self.motivo_cierre,
            self.duracion_operacion,
            id_vela_1m_cierre
        )

        logging.info(f"CloseOperation: {self.ticker} | {self.tipo_operacion} | "
                     f"Resultado={self.resultado:+.2f} | Motivo={motivo}")