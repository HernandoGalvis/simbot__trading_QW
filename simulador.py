# simulador.py (CORREGIDO Y FINAL)

import logging
from datetime import datetime, timedelta
from clases import Inversionista, Operacion
from dao.senales import obtener_senales
from dao.precios import obtener_precio_min_max_close, obtener_id_vela_1m
from dao.estrategias import obtener_parametros_estrategia
from modulos.confirmacion import Confirmador
from modulos.logging_utils import registrar_evento, vaciar_log_a_bd

# ✅ Variable temporal mientras se implementa en BD
PORC_MINIMO_AVANCE_TP_DEFAULT = 0.20  # 20% del camino hacia TP para activar protección

class Simulador:
    def __init__(self, inversionista, fecha_inicio, fecha_fin):
        self.inv = inversionista
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin
        self.timeline = self._generar_timeline()
        self.confirmador = Confirmador()
        self.senales_procesadas = set()  # ✅ Evitar procesar la misma señal dos veces
        self.cache_estrategias = {}  # ✅ Cache para parámetros de estrategias
        logging.info(f"📋 Simulador inicializado para inversión {self.inv.id}")

    def _generar_timeline(self):
        logging.info(f"⏳ Generando línea de tiempo desde {self.fecha_inicio} hasta {self.fecha_fin}")
        ts = self.fecha_inicio
        timeline = []
        while ts <= self.fecha_fin:
            timeline.append(ts)
            ts += timedelta(minutes=1)
        logging.info(f"⏰ Timeline generado: {len(timeline)} minutos")
        self.timeline = timeline
        return timeline

    def _obtener_parametros_estrategia_cached(self, id_estrategia):
        """Obtiene parámetros de estrategia con cache"""
        if id_estrategia not in self.cache_estrategias:
            try:
                self.cache_estrategias[id_estrategia] = obtener_parametros_estrategia(id_estrategia)
            except Exception as e:
                logging.critical(f"❌ ERROR CRÍTICO: No se pueden continuar operaciones sin parámetros de estrategia {id_estrategia}")
                logging.critical(f"Detalles del error: {e}")
                raise  # Re-lanzar el error para detener ejecución
        return self.cache_estrategias[id_estrategia]

    def ejecutar(self):
        logging.info(f"🚀 Iniciando simulación para inversión {self.inv.id}")
        logging.info(f"💰 Capital inicial: {self.inv.capital_actual:.2f}")
        for i, ts in enumerate(self.timeline):
            # Mostrar progreso cada 300 minutos (5 horas)
            if i % 300 == 0:
                logging.info(f"⏳ Procesando minuto: {ts} [{i+1}/{len(self.timeline)}] | Capital: {self.inv.capital_actual:.2f}")

            # 1. Procesar confirmaciones pendientes
            senales_confirmadas = self.confirmador.procesar_cola(ts, self.inv, registrar_evento)
            for sen in senales_confirmadas:
                if sen['id_senal'] in self.senales_procesadas:
                    continue
                logging.info(f"✅ Señal confirmada: {sen['ticker_fk']} | {sen['tipo_senal']} | ID={sen['id_senal']}")
                self._intentar_operar(sen, ts)
                self.senales_procesadas.add(sen['id_senal'])

            # 2. Procesar nuevas señales
            senales = obtener_senales(ts)
            if senales:
                logging.info(f"🔔 Se encontraron {len(senales)} señales para {ts}")
                for sen in senales:
                    if sen['id_senal'] in self.senales_procesadas:
                        continue  # ✅ Evitar procesar la misma señal dos veces
                    usar_confirmacion = False  # placeholder
                    if usar_confirmacion:
                        reglas = [] # obtén de BD
                        self.confirmador.agregar_a_cola(sen, reglas)
                        registrar_evento(
                            inversionista=self.inv,
                            tipo_evento="esperando_confirmacion",
                            id_senal_fk=sen["id_senal"],
                            ticker=sen["ticker_fk"],
                            detalle=f"Esperando confirmación para {sen['ticker_fk']} | {sen['tipo_senal']}"
                        )
                    else:
                        self._intentar_operar(sen, ts)
                        self.senales_procesadas.add(sen['id_senal'])  # ✅ Marcar como procesada

            # 3. Monitorear cierres de operaciones activas
            self._monitorear_cierres(ts)

        # 4. Calcular pyg_no_realizado para operaciones abiertas
        self._calcular_pyg_no_realizado_final()

        # 5. Guardar logs y capital
        logging.info("💾 Guardando logs en base de datos...")
        vaciar_log_a_bd(self.inv)
        logging.info("🏦 Actualizando capital del inversionista en BD...")
        from dao.logs import actualizar_capital_inversionista
        actualizar_capital_inversionista(self.inv.id, self.inv.capital_actual)
        logging.info("✅ Simulación finalizada exitosamente.")
        logging.info(f"📊 Capital final: {self.inv.capital_actual:.2f}")

    def _intentar_operar(self, sen, ts):
        from clases import aplicar_slippage
        # ✅ Verificar y reiniciar contadores diarios
        self.inv.verificar_y_reiniciar_contadores(ts)

        # ✅ Calcular monto de operación respetando límites de tamaño
        # Usar capital_aportado para cálculos de riesgo (no capital_actual)
        riesgo_maximo = self.inv.capital_aportado * (self.inv.riesgo_max_pct / 100)
        # Determinar monto objetivo respetando tamano_min y tamano_max
        monto_objetivo = min(riesgo_maximo, self.inv.tamano_max)
        monto_objetivo = max(self.inv.tamano_min, monto_objetivo)

        # Validar que el monto objetivo sea viable
        if monto_objetivo < self.inv.tamano_min:
            registrar_evento(
                inversionista=self.inv,
                tipo_evento="rechazo",
                id_senal_fk=sen["id_senal"],
                motivo_no_operacion=f"Monto objetivo {monto_objetivo:.2f} menor al mínimo permitido ({self.inv.tamano_min})",
                detalle=f"Operación rechazada: {sen['ticker_fk']} | {sen['tipo_senal']} | monto_objetivo={monto_objetivo:.2f} < min={self.inv.tamano_min}",
                timestamp_evento=sen['timestamp_senal']
            )
            return

        # Validar capital disponible
        if self.inv.capital_actual < monto_objetivo:
            registrar_evento(
                inversionista=self.inv,
                tipo_evento="rechazo",
                id_senal_fk=sen["id_senal"],
                motivo_no_operacion="Sin capital suficiente",
                detalle=f"Sin capital: necesario={monto_objetivo:.2f}, disponible={self.inv.capital_actual:.2f}",
                timestamp_evento=sen['timestamp_senal']
            )
            return

        # ✅ Validar límite diario de operaciones
        if self.inv.operaciones_hoy >= self.inv.limite_diario:
            registrar_evento(
                inversionista=self.inv,
                tipo_evento="rechazo",
                id_senal_fk=sen["id_senal"],
                motivo_no_operacion="Límite diario de operaciones alcanzado",
                detalle=f"Límite diario alcanzado: {self.inv.operaciones_hoy}/{self.inv.limite_diario}",
                timestamp_evento=sen['timestamp_senal']
            )
            return

        # ✅ Validar límite de operaciones activas (solo para NUEVAS operaciones, no para DCA)
        clave = f"{sen['ticker_fk']}-{sen['tipo_senal']}"
        if clave not in self.inv.operaciones_activas:  # Solo para nuevas operaciones
            if len(self.inv.operaciones_activas) >= self.inv.limite_abiertas:
                registrar_evento(
                    inversionista=self.inv,
                    tipo_evento="rechazo",
                    id_senal_fk=sen["id_senal"],
                    motivo_no_operacion="Límite de operaciones abiertas alcanzado",
                    detalle=f"Límite de operaciones abiertas alcanzado: {len(self.inv.operaciones_activas)}/{self.inv.limite_abiertas}",
                    timestamp_evento=sen['timestamp_senal']
                )
                return

        # ✅ Obtener high, low y close de la vela de 1 minuto
        high, low, close = obtener_precio_min_max_close(sen['ticker_fk'], ts)
        if not close:
            registrar_evento(
                inversionista=self.inv,
                tipo_evento="rechazo",
                id_senal_fk=sen["id_senal"],
                motivo_no_operacion="Vela no encontrada en ohlcv_raw_1m",
                detalle=f"Vela no encontrada: {sen['ticker_fk']} | {ts}",
                timestamp_evento=sen['timestamp_senal']
            )
            return

        # Aplicar slippage al precio de cierre
        try:
            precio_con_slippage = aplicar_slippage(close, self.inv.slippage_pct, sen['tipo_senal'])
        except Exception as e:
            registrar_evento(
                inversionista=self.inv,
                tipo_evento="rechazo",
                id_senal_fk=sen["id_senal"],
                motivo_no_operacion="Error al aplicar slippage",
                detalle=f"Error en slippage: {e}",
                timestamp_evento=sen['timestamp_senal']
            )
            return

        # Clave única por ticker + tipo
        clave = f"{sen['ticker_fk']}-{sen['tipo_senal']}"
        if clave in self.inv.operaciones_activas:
            # DCA: Acumular en operación existente
            op = self.inv.operaciones_activas[clave]
            # ✅ Verificar que el DCA no exceda el tamaño máximo permitido
            capital_actual_op = op.capital_riesgo_usado
            capital_maximo_op = self.inv.tamano_max
            capital_disponible_para_dca = capital_maximo_op - capital_actual_op
            if capital_disponible_para_dca <= 0:
                # Ya se alcanzó el límite máximo para esta operación
                registrar_evento(
                    inversionista=self.inv,
                    tipo_evento="rechazo",
                    id_senal_fk=sen["id_senal"],
                    motivo_no_operacion="Límite de tamaño máximo de operación alcanzado",
                    detalle=f"DCA rechazado: {sen['ticker_fk']} | {sen['tipo_senal']} | Límite operación={capital_maximo_op:.2f} | Actual={capital_actual_op:.2f}",
                    timestamp_evento=sen['timestamp_senal']
                )
                return

            # Calcular monto para DCA (respetando límite)
            monto_dca = min(monto_objetivo, capital_disponible_para_dca)
            # Validar que haya suficiente capital disponible
            if self.inv.capital_actual < monto_dca:
                registrar_evento(
                    inversionista=self.inv,
                    tipo_evento="rechazo",
                    id_senal_fk=sen["id_senal"],
                    motivo_no_operacion="Sin capital suficiente para DCA",
                    detalle=f"Sin capital para DCA: necesario={monto_dca:.2f}, disponible={self.inv.capital_actual:.2f}",
                    timestamp_evento=sen['timestamp_senal']
                )
                return

            cantidad_dca = monto_dca / precio_con_slippage
            # ✅ Aplicar DCA con monto validado
            op.aplicar_dca(self.inv, precio_con_slippage, cantidad_dca)
            self.inv.capital_actual -= monto_dca
            self.inv.operaciones_hoy += 1
            # ✅ Registrar evento de DCA
            registrar_evento(
                inversionista=self.inv,
                tipo_evento="dca",
                id_operacion_fk=op.id_operacion,
                id_senal_fk=sen["id_senal"],
                ticker=sen["ticker_fk"],
                tipo_operacion=sen["tipo_senal"],
                cantidad=cantidad_dca,
                precio_entrada=precio_con_slippage,
                capital_antes=self.inv.capital_actual + monto_dca,
                capital_despues=self.inv.capital_actual,
                detalle=f"DCA en {sen['ticker_fk']} | +{cantidad_dca:.6f} @ {precio_con_slippage} | Monto={monto_dca:.2f}",
                timestamp_evento=sen['timestamp_senal'],
                precio_senal=sen.get('precio_senal'),
                sl=sen.get('stop_loss_price'),
                tp=sen.get('target_profit_price'),
                id_estrategia_fk=sen.get('id_estrategia_fk'),
                porc_sl=abs((sen.get('precio_senal', precio_con_slippage) - sen.get('stop_loss_price', 0)) / sen.get('precio_senal', precio_con_slippage)) * 100 if sen.get('precio_senal') and sen.get('stop_loss_price') else None,
                porc_tp=abs((sen.get('target_profit_price', 0) - sen.get('precio_senal', precio_con_slippage)) / sen.get('precio_senal', precio_con_slippage)) * 100 if sen.get('precio_senal') and sen.get('target_profit_price') else None,
                nro_operacion=op.cnt_operaciones,
                id_vela_1m_apertura=op.id_vela_1m_apertura  # ✅ Registrar ID de vela de apertura en el log
            )
            logging.info(f"🔁 DCA aplicado: {sen['ticker_fk']} | {sen['tipo_senal']} | +{cantidad_dca:.6f} @ {precio_con_slippage} | Monto={monto_dca:.2f}")
        else:
            # Nueva operación: usar monto objetivo validado
            monto_operacion = monto_objetivo
            # Validar capital disponible para nueva operación
            if self.inv.capital_actual < monto_operacion:
                registrar_evento(
                    inversionista=self.inv,
                    tipo_evento="rechazo",
                    id_senal_fk=sen["id_senal"],
                    motivo_no_operacion="Sin capital suficiente para nueva operación",
                    detalle=f"Sin capital para nueva operación: necesario={monto_operacion:.2f}, disponible={self.inv.capital_actual:.2f}",
                    timestamp_evento=sen['timestamp_senal']
                )
                return

            cantidad = monto_operacion / precio_con_slippage
            # ✅ Decidir apalancamiento según configuración del inversionista
            if self.inv.usar_parametros_senal:
                apal_senal = sen.get('apalancamiento_calculado', 1)
                apal = min(apal_senal, self.inv.apalancamiento_max)
            else:
                apal = self.inv.apalancamiento_max

            # ✅ Obtener id_vela_1m_apertura para registrar en la operación
            id_vela_apertura = obtener_id_vela_1m(sen['ticker_fk'], ts)
            if not id_vela_apertura:
                registrar_evento(
                    inversionista=self.inv,
                    tipo_evento="rechazo",
                    id_senal_fk=sen["id_senal"],
                    motivo_no_operacion="ID de vela de apertura no encontrado",
                    detalle=f"ID de vela no encontrado: {sen['ticker_fk']} | {ts}",
                    timestamp_evento=sen['timestamp_senal']
                )
                return

            op = Operacion(
                id_senal=sen['id_senal'],
                ticker=sen['ticker_fk'],
                tipo=sen['tipo_senal'],
                precio=precio_con_slippage,
                cant=cantidad,
                apal=apal,
                sl=sen['stop_loss_price'],
                tp=sen['target_profit_price'],
                padre=None,
                id_inversionista=self.inv.id,
                id_estrategia_fk=sen['id_estrategia_fk'],
                timestamp_apertura=sen['timestamp_senal'],
                id_vela_1m_apertura=id_vela_apertura  # ✅ Agregar ID de vela de apertura
            )
            self.inv.operaciones_activas[clave] = op
            self.inv.capital_actual -= monto_operacion
            self.inv.operaciones_hoy += 1
            registrar_evento(
                inversionista=self.inv,
                tipo_evento="apertura",
                id_operacion_fk=op.id_operacion,
                id_senal_fk=sen["id_senal"],
                ticker=sen["ticker_fk"],
                tipo_operacion=sen["tipo_senal"],
                cantidad=cantidad,
                precio_entrada=precio_con_slippage,
                capital_antes=self.inv.capital_actual + monto_operacion,
                capital_despues=self.inv.capital_actual,
                detalle=f"Apertura: {sen['ticker_fk']} | {sen['tipo_senal']} | {cantidad:.6f} @ {precio_con_slippage} | Monto={monto_operacion:.2f}",
                timestamp_evento=sen['timestamp_senal'],
                precio_senal=sen.get('precio_senal'),
                sl=sen.get('stop_loss_price'),
                tp=sen.get('target_profit_price'),
                id_estrategia_fk=sen.get('id_estrategia_fk'),
                porc_sl=abs((sen.get('precio_senal', precio_con_slippage) - sen.get('stop_loss_price', 0)) / sen.get('precio_senal', precio_con_slippage)) * 100 if sen.get('precio_senal') and sen.get('stop_loss_price') else None,
                porc_tp=abs((sen.get('target_profit_price', 0) - sen.get('precio_senal', precio_con_slippage)) / sen.get('precio_senal', precio_con_slippage)) * 100 if sen.get('precio_senal') and sen.get('target_profit_price') else None,
                nro_operacion=op.cnt_operaciones,
                id_vela_1m_apertura=id_vela_apertura  # ✅ Registrar ID de vela de apertura en el log
            )
            logging.info(f"🆕 Apertura: {sen['ticker_fk']} | {sen['tipo_senal']} | {cantidad:.6f} @ {precio_con_slippage} | Monto={monto_operacion:.2f} | Vela ID={id_vela_apertura}")

    def _monitorear_cierres(self, ts):
        """
        Monitorea todas las operaciones activas para verificar cierres por:
        - TP
        - Retroceso desde entrada
        - Retroceso desde máximo (CON PROTECCIÓN DE GANANCIAS MÍNIMA)
        - Cierre parcial por SL
        - SL (Stop Loss Total)
        """
        activas = list(self.inv.operaciones_activas.values())
        for op in activas:
            clave_op = f"{op.ticker}-{op.tipo_operacion}" # Clave única para operar en el diccionario
            high, low, close = obtener_precio_min_max_close(op.ticker, ts)
            if not high or not low or not close:
                continue

            # Convertir a float
            high = float(high)
            low = float(low)
            close = float(close)

            # Actualizar precios extremos con close
            op.actualizar_precio(close, ts)

            # ✅ Obtener id_vela_1m_cierre una sola vez
            id_vela = obtener_id_vela_1m(op.ticker, ts)

            # ✅ Obtener parámetros de la estrategia asociada a la operación
            try:
                params = self._obtener_parametros_estrategia_cached(op.id_estrategia_fk)
                porc_retroceso_entrada = params['porc_limite_retro_entrada']
                porc_retroceso_max = params['porc_limite_retro']
                porc_retroceso_parcial = params['porc_retroceso_liquidacion_sl']  # <-- Este es un porcentaje (ej: 0.4 para 40%)
                porc_liquidacion = params['porc_liquidacion_parcial_sl']  # <-- Este es un porcentaje (ej: 0.5 para 50%)
            except Exception as e:
                logging.critical(f"❌ ERROR CRÍTICO: Imposible continuar monitoreo de cierres para operación {op.id_operacion}")
                raise  # Detener ejecución

            # --- Cierre por TP ---
            if (op.tipo_operacion == "LONG" and high >= op.take_profit) or \
               (op.tipo_operacion == "SHORT" and low <= op.take_profit):
                op.cerrar_total(self.inv, close, "Take Profit", ts, id_vela)
                if clave_op in self.inv.operaciones_activas:
                    del self.inv.operaciones_activas[clave_op]
                logging.info(f"🎯 TP alcanzado: {op.ticker} | {op.tipo_operacion} | Cerrada")
                continue  # Pasar a la siguiente operación

            # --- Cierre por retroceso desde entrada ---
            # Se evalúa antes del SL total
            if op.tipo_operacion == "LONG":
                retroceso_desde_entrada = (op.precio_entrada - low) / op.precio_entrada
                if retroceso_desde_entrada >= porc_retroceso_entrada:
                    op.cerrar_total(self.inv, close, "Retroceso desde apertura", ts, id_vela)
                    if clave_op in self.inv.operaciones_activas:
                        del self.inv.operaciones_activas[clave_op]
                    logging.warning(f"📉 Retroceso desde entrada: {op.ticker} | Cerrada")
                    continue  # Pasar a la siguiente operación
            elif op.tipo_operacion == "SHORT":
                retroceso_desde_entrada = (high - op.precio_entrada) / op.precio_entrada
                if retroceso_desde_entrada >= porc_retroceso_entrada:
                    op.cerrar_total(self.inv, close, "Retroceso desde apertura", ts, id_vela)
                    if clave_op in self.inv.operaciones_activas:
                        del self.inv.operaciones_activas[clave_op]
                    logging.warning(f"📈 Retroceso desde entrada: {op.ticker} | Cerrada")
                    continue  # Pasar a la siguiente operación

            # --- Cierre por retroceso desde máximo (CON PROTECCIÓN DE GANANCIAS MÍNIMA) ---
            # Se evalúa antes del SL total
            protegido = False # Bandera para evitar evaluar SL parcial si ya se activó esta protección
            if op.tipo_operacion == "LONG":
                if op.precio_max_alcanzado > op.precio_entrada:
                    # ✅ Verificar si se ha alcanzado el avance mínimo hacia TP para activar protección
                    distancia_al_tp = op.take_profit - op.precio_entrada
                    avance_minimo_requerido = PORC_MINIMO_AVANCE_TP_DEFAULT * distancia_al_tp
                    precio_minimo_activacion = op.precio_entrada + avance_minimo_requerido
                    # Solo activar protección si se ha alcanzado el mínimo requerido
                    if op.precio_max_alcanzado >= precio_minimo_activacion:
                        protegido = True # Activar bandera
                        # Calcular distancia entre máximo alcanzado y precio de entrada (ganancia potencial)
                        distancia_ganancia = op.precio_max_alcanzado - op.precio_entrada
                        # Calcular retroceso permitido según parámetro de estrategia
                        retroceso_permitido = porc_retroceso_max * distancia_ganancia
                        # Calcular precio mínimo antes de cierre
                        precio_minimo_permitido = op.precio_max_alcanzado - retroceso_permitido
                        # Verificar si el precio actual está por debajo del mínimo permitido
                        if low <= precio_minimo_permitido:
                            op.cerrar_total(self.inv, close, "Retroceso desde máximo", ts, id_vela)
                            if clave_op in self.inv.operaciones_activas:
                                del self.inv.operaciones_activas[clave_op]
                            logging.warning(f"🔻 Retroceso desde máximo: {op.ticker} | Cerrada | Max={op.precio_max_alcanzado:.6f} | MinPermitido={precio_minimo_permitido:.6f} | Actual={low:.6f}")
                            continue  # Pasar a la siguiente operación
                    else:
                        # No hay ganancia suficiente para activar protección
                        logging.debug(f"🔒 Protección desactivada: {op.ticker} | Max={op.precio_max_alcanzado:.6f} < MinReq={precio_minimo_activacion:.6f}")
            elif op.tipo_operacion == "SHORT":
                if op.precio_min_alcanzado < op.precio_entrada:
                    # ✅ Verificar si se ha alcanzado el avance mínimo hacia TP para activar protección
                    distancia_al_tp = op.precio_entrada - op.take_profit
                    avance_minimo_requerido = PORC_MINIMO_AVANCE_TP_DEFAULT * distancia_al_tp
                    precio_maximo_activacion = op.precio_entrada - avance_minimo_requerido
                    # Solo activar protección si se ha alcanzado el mínimo requerido
                    if op.precio_min_alcanzado <= precio_maximo_activacion:
                        protegido = True # Activar bandera
                        # Calcular distancia entre precio de entrada y mínimo alcanzado (ganancia potencial)
                        distancia_ganancia = op.precio_entrada - op.precio_min_alcanzado
                        # Calcular retroceso permitido según parámetro de estrategia
                        retroceso_permitido = porc_retroceso_max * distancia_ganancia
                        # Calcular precio máximo antes de cierre
                        precio_maximo_permitido = op.precio_min_alcanzado + retroceso_permitido
                        # Verificar si el precio actual está por encima del máximo permitido
                        if high >= precio_maximo_permitido:
                            op.cerrar_total(self.inv, close, "Retroceso desde mínimo", ts, id_vela)
                            if clave_op in self.inv.operaciones_activas:
                                del self.inv.operaciones_activas[clave_op]
                            logging.warning(f"🔺 Retroceso desde mínimo: {op.ticker} | Cerrada | Min={op.precio_min_alcanzado:.6f} | MaxPermitido={precio_maximo_permitido:.6f} | Actual={high:.6f}")
                            continue  # Pasar a la siguiente operación
                    else:
                        # No hay ganancia suficiente para activar protección
                        logging.debug(f"🔒 Protección desactivada: {op.ticker} | Min={op.precio_min_alcanzado:.6f} > MaxReq={precio_maximo_activacion:.6f}")

            # --- Cierre parcial por SL ---
            # Solo se evalúa si NO es una operación hija y si no se activó la protección de retroceso desde máximo
            # Se evalúa antes del SL total
            es_hija = getattr(op, 'es_operacion_hija', False)
            if not es_hija and not protegido:
                 if op.tipo_operacion == "LONG":
                     retroceso_para_parcial = (op.precio_entrada - low) / op.precio_entrada
                     if retroceso_para_parcial >= porc_retroceso_parcial:
                         # Importante: No usar 'continue' después de cerrar_parcial
                         # porque queremos que la operación hija creada sea monitoreada
                         # en las siguientes iteraciones del bucle principal (en la próxima vela)
                         op.cerrar_parcial(self.inv, close, porc_liquidacion)
                         # La operación original se marca como 'cerrada_parcial' y se elimina del diccionario
                         # La operación hija se inserta en el diccionario con la misma clave
                         # Por lo tanto, no necesitamos eliminar explícitamente la original,
                         # porque cerrar_parcial ya reemplaza la entrada en el diccionario.
                         # Sin embargo, por claridad y seguridad, la eliminamos.
                         # Aunque técnicamente no es necesaria porque cerrar_parcial lo hace,
                         # lo hacemos explícitamente para evitar confusiones.
                         # La operación hija ya está en el diccionario gracias a cerrar_parcial.
                         if clave_op in self.inv.operaciones_activas:
                             # La operación hija ya debería estar en el diccionario, pero por si acaso
                             # la original aún está (aunque cerrar_parcial debería haberla reemplazado)
                             # Esta línea asegura que cualquier referencia vieja se elimine.
                             # Pero como cerrar_parcial reemplaza la clave, esto podría ser redundante.
                             # La clave punto es que NO usamos 'continue' aquí.
                             pass # No hacer nada, la lógica de cerrar_parcial maneja el diccionario
                         logging.warning(f"⚠️  Cierre parcial por SL: {op.ticker} | {porc_liquidacion}% liquidado")
                         # NO usar 'continue' aquí. Permitir que el bucle siga, aunque en la práctica,
                         # la operación original ya no está en la lista 'activas' de esta iteración del for.
                         # La nueva operación hija será procesada en la próxima iteración del timeline.
                         # El 'continue' implícito al final del bucle es suficiente.

                 elif op.tipo_operacion == "SHORT":
                     retroceso_para_parcial = (high - op.precio_entrada) / op.precio_entrada
                     if retroceso_para_parcial >= porc_retroceso_parcial:
                         op.cerrar_parcial(self.inv, close, porc_liquidacion)
                         # Mismo manejo del diccionario que para LONG
                         if clave_op in self.inv.operaciones_activas:
                             pass # Manejado por cerrar_parcial
                         logging.warning(f"⚠️  Cierre parcial por SL: {op.ticker} | {porc_liquidacion}% liquidado")
                         # NO usar 'continue'

            # --- Cierre por SL (Stop Loss Total) ---
            # Esta es la condición de último recurso, evaluada después de todas las demás.
            if (op.tipo_operacion == "LONG" and low <= op.stop_loss) or \
               (op.tipo_operacion == "SHORT" and high >= op.stop_loss):
                op.cerrar_total(self.inv, close, "Stop Loss", ts, id_vela)
                if clave_op in self.inv.operaciones_activas:
                    del self.inv.operaciones_activas[clave_op]
                logging.info(f"🛑 SL alcanzado: {op.ticker} | {op.tipo_operacion} | Cerrada")
                continue  # Pasar a la siguiente operación

        # Fin del bucle for op in activas

    def _calcular_pyg_no_realizado_final(self):
        """
        Calcula el pyg_no_realizado para operaciones abiertas al final de la simulación.
        """
        for op in self.inv.operaciones_activas.values():
            high, low, close = obtener_precio_min_max_close(op.ticker, self.fecha_fin)
            if not close:
                continue
            close = float(close)
            if op.tipo_operacion == "LONG":
                op.pyg_no_realizado = (close - op.precio_entrada) * op.cantidad
            else:
                op.pyg_no_realizado = (op.precio_entrada - close) * op.cantidad
            # Actualizar en BD
            from dao.operaciones import actualizar_pyg_no_realizado
            actualizar_pyg_no_realizado(op.id_operacion, op.pyg_no_realizado)