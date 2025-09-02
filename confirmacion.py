# modulos/confirmacion.py
from dao.precios import obtener_precio_min_max_close  # ✅ Import corregido
import logging

class Confirmador:
    def __init__(self):
        self.cola = []  # [ { senal, reglas, ts_entrada }, ... ]

    def agregar_a_cola(self, senal, reglas):
        """
        Agrega una señal a la cola de confirmación.
        """
        self.cola.append({
            'senal': senal,
            'reglas': reglas,
            'ts_entrada': None  # Se asigna al procesar
        })
        logging.info(f"🕒 Señal ID={senal['id_senal']} agregada a cola de confirmación")

    def procesar_cola(self, ts_actual, inversionista, registrar_evento):
        """
        Procesa todas las señales en cola que cumplan sus condiciones de confirmación.
        Devuelve lista de señales confirmadas.
        """
        senales_confirmadas = []
        activas = []

        for item in self.cola:
            senal = item['senal']
            reglas = item['reglas']

            # Si no tiene timestamp de entrada, se asigna ahora
            if item['ts_entrada'] is None:
                item['ts_entrada'] = ts_actual

            # Calcular tiempo en cola (en minutos)
            delta = (ts_actual - item['ts_entrada']).total_seconds() / 60

            # Aplicar reglas de confirmación
            cumple = True

            for regla in reglas:
                tipo = regla['tipo']
                valor = regla['valor']

                if tipo == "tiempo_max_espera":
                    if delta > valor:
                        cumple = False
                        registrar_evento(
                            inversionista=inversionista,
                            tipo_evento="rechazo_confirmacion",
                            id_senal_fk=senal["id_senal"],
                            motivo_no_operacion=f"Tiempo de espera excedido: {delta:.1f} min > {valor} min"
                        )
                        logging.info(f"❌ Señal {senal['id_senal']} rechazada por tiempo de espera")
                        break

                elif tipo == "volumen_min":
                    # Ejemplo: requiere volumen mínimo en el periodo
                    pass  # Implementar si es necesario

                elif tipo == "precio_supera":
                    # Ejemplo: precio debe superar un nivel desde entrada
                    high, low, close = obtener_precio_min_max_close(senal['ticker'], ts_actual)
                    if not high:
                        continue
                    precio_referencia = item['ts_entrada_precio']  # Debería estar guardado
                    if high <= precio_referencia * (1 + valor / 100):
                        cumple = False
                        break

                # Añade más reglas según necesidad

            if cumple:
                senales_confirmadas.append(senal)
                logging.info(f"✅ Señal {senal['id_senal']} confirmada tras {delta:.1f} min")
                registrar_evento(
                    inversionista=inversionista,
                    tipo_evento="senal_confirmada",
                    id_senal_fk=senal["id_senal"],
                    ticker=senal["ticker"],
                    detalle=f"Señal confirmada tras {delta:.1f} minutos"
                )
            else:
                activas.append(item)

        # Actualizar cola: solo las no confirmadas
        self.cola = activas

        return senales_confirmadas