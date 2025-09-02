# main.py
import logging
from datetime import datetime
from clases import Inversionista
from dao.inversionistas import obtener_todos_inversionistas_activos
from simulador import Simulador
from db_connection import conectar_db

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('simulador.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def main():
    logging.info("🟢 Iniciando simulador de trading...")
    
    # 1. Definir rango de simulación
    fecha_inicio = datetime(2025, 1, 1, 0, 0, 0)
    fecha_fin = datetime(2025, 3, 1, 0, 0, 0)
    
    logging.info(f"📅 Rango de simulación: {fecha_inicio} → {fecha_fin}")
    
    # 2. Obtener todos los inversionistas activos
    inversionistas_configs = obtener_todos_inversionistas_activos()
    
    if not inversionistas_configs:
        logging.error("❌ No se encontraron inversionistas activos")
        return
    
    logging.info(f"👥 Procesando {len(inversionistas_configs)} inversionistas activos")
    
    # 3. Procesar cada inversionista
    for config in inversionistas_configs:
        id_inversionista = config['id_inversionista']
        capital_inicial = config['capital_aportado']
        
        logging.info(f"💼 Cargando inversión: ID={id_inversionista}")
        logging.info(f"💰 Capital inicial: {capital_inicial:.2f}")
        
        # Crear instancia del inversionista
        inv = Inversionista(id_inv=id_inversionista, capital=capital_inicial, config=config)
        
        # Inicializar y ejecutar simulador
        sim = Simulador(inversionista=inv, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin)
        logging.info(f"⚙️  Ejecutando simulador para inversionista {id_inversionista}...")
        sim.ejecutar()
        
        logging.info(f"✅ Simulación completada para inversionista {id_inversionista}")


if __name__ == "__main__":
    main()