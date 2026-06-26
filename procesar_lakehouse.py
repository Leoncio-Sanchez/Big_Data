#!/usr/bin/env python3  # shebang para ejecutar directamente
"""
ETAPA 2 (Silver) + ETAPA 3 (Gold) — Pipeline PySpark sobre YARN
═══════════════════════════════════════════════════════════════
Cluster ZeroTier: 1 Driver (leo) + 3 Workers (XUBUNTU, DEBIAN, isait-VirtualBox)
Ejecución: spark-submit --master yarn --deploy-mode client procesar_lakehouse.py
"""

import sys  # funciones del sistema como sys.exit
import logging  # logging estructurado con niveles
from typing import Optional, Tuple  # tipado: Optional (puede ser None), Tuple

# ─── Configuración del cluster ZeroTier ───
NAMENODE_RPC = "hdfs://10.61.61.105:9000"  # URI del NameNode HDFS (IP ZeroTier del master)
BRONZE_PATH = f"{NAMENODE_RPC}/lakehouse/bronze/yellow_tripdata_2023-01.parquet"  # ruta HDFS del parquet crudo
SILVER_PATH = f"{NAMENODE_RPC}/lakehouse/silver/taxis_limpio"  # ruta HDFS donde se guardará la capa silver
GOLD_BASE = f"{NAMENODE_RPC}/lakehouse/gold"  # ruta base HDFS para los KPIs (capa gold)

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"  # formato de log con nombre del logger
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)  # configura logging a INFO con ese formato
log = logging.getLogger("lakehouse-spark")  # crea logger con nombre "lakehouse-spark"


# ═══════════════════════════════════════════════════════════════
# 1. VERIFICACIÓN DE ENTORNO
# ═══════════════════════════════════════════════════════════════

def verificar_entorno() -> bool:  # verifica que PySpark esté instalado
    """
    Valida que PySpark esté disponible antes de instanciar la SparkSession.
    Si falta, emite un mensaje con el comando de instalación y retorna False.
    """
    try:  # intenta importar pyspark
        import pyspark  # importa la librería PySpark
        log.info("✓ PySpark %s importado correctamente", pyspark.__version__)  # loggea versión instalada
        return True  # retorna True, entorno listo
    except ImportError:  # si PySpark no está instalado
        log.error("✗ PySpark no encontrado en el entorno.")  # loggea error
        log.error("  Instálalo con: pip install pyspark")  # muestra comando de instalación
        return False  # retorna False


# ═══════════════════════════════════════════════════════════════
# 2. SPARKSESSION OPTIMIZADA PARA ZEROTIER + YARN
# ═══════════════════════════════════════════════════════════════

def crear_spark() -> "SparkSession":  # crea y configura la SparkSession para YARN
    """
    Crea una SparkSession configurada para el cluster ZeroTier.
    - Driver en leo (10.61.61.105), sin trabajo pesado local.
    - 3 Workers × 4 GB RAM + 2 cores.
    - Serialización Kryo, AQE, timeouts ajustados a la VPN.
    """
    from pyspark.sql import SparkSession  # importa SparkSession solo aquí (evita error si no está instalado)

    log.info("Construyendo SparkSession para YARN sobre ZeroTier…")  # log inicio de construcción

    spark = (SparkSession.builder  # builder pattern para construir la sesión
        .appName("Lakehouse-Silver-Gold-ZeroTier")  # nombre de la aplicación Spark
        .master("yarn")  # modo de ejecución: YARN (cluster manager)

        # ── Conectividad ZeroTier ──
        .config("spark.yarn.access.namenodes", NAMENODE_RPC)  # dice a YARN qué NameNode usar
        .config("spark.driver.host", "10.61.61.105")  # IP del driver (accesible desde workers vía ZeroTier)
        .config("spark.driver.bindAddress", "10.61.61.105")  # interfaz donde el driver escucha

        # ── Event Log para History Server (:18080) ──
        .config("spark.eventLog.enabled", "true")  # habilita bitácora de eventos de Spark
        .config("spark.eventLog.dir", "hdfs://10.61.61.105:9000/spark-logs")  # directorio HDFS persistente para event logs
        .config("spark.history.fs.logDirectory", "hdfs://10.61.61.105:9000/spark-logs")  # History Server lee logs desde HDFS

        # ── Recursos: 3 workers × (4 GB + 2 cores) ──
        .config("spark.executor.instances", "3")  # número de workers (executors) = 3
        .config("spark.executor.memory", "4g")  # 4 GB de RAM por executor
        .config("spark.executor.cores", "2")  # 2 cores (hilos) por executor
        .config("spark.executor.memoryOverhead", "1g")  # 1 GB extra para overhead (Java NIO, etc.)
        .config("spark.driver.memory", "2g")  # 2 GB de RAM para el driver
        .config("spark.driver.cores", "1")  # 1 core para el driver

        # ── Optimizaciones de ejecución ──
        .config("spark.sql.adaptive.enabled", "true")  # AQE: Optimización adaptativa de consultas
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")  # fusiona particiones pequeñas automáticamente
        .config("spark.sql.adaptive.skewJoin.enabled", "true")  # detecta y optimiza joins con datos sesgados
        .config("spark.sql.shuffle.partitions", "12")  # número de particiones para shuffles (3 workers × 2 cores × 2)
        .config("spark.default.parallelism", "12")  # paralelismo default para RDDs
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # Kryo: serialización más rápida que Java
        .config("spark.kryoserializer.buffer.max", "512m")  # buffer máximo de Kryo = 512 MB

        # ── Timeouts holgados para red ZeroTier ──
        .config("spark.network.timeout", "800s")  # timeout general de red (VPN puede ser lenta)
        .config("spark.executor.heartbeatInterval", "60s")  # intervalo de heartbeat executors → driver

        .getOrCreate())  # obtiene sesión existente o crea una nueva

    spark.sparkContext.setLogLevel("WARN")  # reduce ruido en logs (solo muestra WARN y ERROR de Spark interno)
    log.info("✓ SparkSession activa — Driver: 10.61.61.105 | Workers: 3 × (4 GB, 2 cores)")  # confirma creación
    log.info("  Shuffle partitions: 12 | Serializador: Kryo | AQE: habilitado")  # resume config

    return spark  # retorna la SparkSession lista para usar


# ═══════════════════════════════════════════════════════════════
# 3. ETAPA 2 — CAPA PLATA (Limpieza distribuida)
# ═══════════════════════════════════════════════════════════════

def etapa2_silver(spark: "SparkSession") -> Optional["DataFrame"]:  # lee bronze, limpia y escribe silver
    """
    Lee el Parquet crudo desde Bronze, aplica reglas de calidad,
    y escribe la capa Silver particionada por PULocationID.

    Returns
    -------
    DataFrame | None
        DataFrame limpio si todo fue exitoso; None en caso de error.
    """
    from pyspark.sql import functions as F  # importa funciones SQL de Spark (col, sum, avg, etc.)
    from pyspark.sql import DataFrame  # importa el tipo DataFrame para type hints

    log.info("═══ ETAPA 2: BRONZE → SILVER ═══")  # encabezado de etapa
    log.info("Origen:  %s", BRONZE_PATH)  # muestra ruta de origen
    log.info("Destino: %s", SILVER_PATH)  # muestra ruta de destino

    try:  # bloque try para capturar errores
        df = spark.read.parquet(BRONZE_PATH)  # lee el archivo Parquet desde HDFS (capa bronze)
        raw_count = df.count()  # cuenta registros totales leídos (acción que lanza el cómputo)
        log.info("Registros leídos desde Bronze: %d", raw_count)  # loggea cantidad de registros

        if raw_count == 0:  # si no hay datos
            log.warning("Bronze retornó 0 registros. Abortando etapa Silver.")  # advierte
            return None  # retorna None, no hay nada que procesar

        log.info("Aplicando reglas de calidad…")  # log inicio de limpieza
        df_clean = (df  # sobre el DataFrame original
            .filter(  # Filtro (a): elimina registros con datos inválidos
                (F.col("passenger_count") > 0) &  # pasajeros debe ser > 0
                (F.col("trip_distance") > 0)  # distancia debe ser > 0
            )
            .withColumn("tpep_pickup_datetime",  # Transformación (b1): columna pickup a timestamp
                        F.col("tpep_pickup_datetime").cast("timestamp"))  # casteo explícito a timestamp
            .withColumn("tpep_dropoff_datetime",  # Transformación (b2): columna dropoff a timestamp
                        F.col("tpep_dropoff_datetime").cast("timestamp"))  # casteo explícito a timestamp
            .withColumn("duracion_minutos",  # Transformación (c): columna calculada duración del viaje
                        (F.unix_timestamp("tpep_dropoff_datetime") -  # timestamp dropoff en segundos
                         F.unix_timestamp("tpep_pickup_datetime")) / 60.0)  # menos timestamp pickup, dividido 60
            .filter(F.col("duracion_minutos") > 0)  # Filtro (d): solo viajes con duración positiva
        )

        clean_count = df_clean.count()  # cuenta registros después de limpieza
        eliminados = raw_count - clean_count  # calcula cuántos registros se descartaron
        pct_eliminados = (eliminados * 100.0 / raw_count) if raw_count > 0 else 0.0  # % de descartados
        log.info("Registros tras limpieza: %d (descartados: %d / %.1f%%)",  # loggea resumen
                 clean_count, eliminados, pct_eliminados)

        if clean_count == 0:  # si después de limpiar no queda nada
            log.warning("Tras limpieza quedan 0 registros. Abortando escritura Silver.")  # advierte
            return None  # retorna None

        log.info("Escribiendo capa Silver (Parquet particionado por PULocationID)…")  # log inicio escritura
        (df_clean  # DataFrame limpio
            .write  # modo escritura
            .mode("overwrite")  # sobrescribe si ya existe
            .partitionBy("PULocationID")  # particiona por zona de recogida (mejora consultas futuras)
            .parquet(SILVER_PATH))  # guarda como Parquet en HDFS

        log.info("✓ ETAPA 2 completada. Silver guardado en: %s", SILVER_PATH)  # log éxito
        return df_clean  # retorna el DataFrame limpio para la etapa gold

    except Exception as e:  # captura cualquier error
        log.exception("✗ ETAPA 2 falló: %s", e)  # loggea error con traceback
        return None  # retorna None


# ═══════════════════════════════════════════════════════════════
# 4. ETAPA 3 — CAPA ORO (KPIs para Power BI)
# ═══════════════════════════════════════════════════════════════

def etapa3_gold(spark: "SparkSession", df_silver: "DataFrame") -> bool:  # calcula 3 KPIs desde silver
    """
    Calcula 3 KPIs de negocio a partir de la capa Silver y los guarda
    como CSV con cabeceras en HDFS para consumo directo desde Power BI.

    - KPI 1 (Financiero): Ingreso total y propina promedio por hora.
    - KPI 2 (Operativo):   Duración y distancia promedio por nº de pasajeros.
    - KPI 3 (Demanda):     Total de viajes por zona de recogida (descendente).
    """
    from pyspark.sql import functions as F  # importa funciones SQL

    log.info("═══ ETAPA 3: SILVER → GOLD ═══")  # encabezado de etapa
    log.info("Destino base: %s", GOLD_BASE)  # muestra ruta base de gold

    try:  # bloque try para capturar errores
        # ──────────────────────────────────────────────────────
        # KPI 1 — FINANCIERO
        #   Ingreso total (total_amount) y propina promedio (tip_amount)
        #   agrupados por hora del día de recogida.
        # ──────────────────────────────────────────────────────
        log.info("Calculando KPI 1 — Financiero (por hora del día)…")  # log inicio KPI 1
        kpi_financiero = (df_silver  # sobre silver
            .withColumn("hora", F.hour("tpep_pickup_datetime"))  # extrae la hora (0-23) de la recogida
            .groupBy("hora")  # agrupa por hora
            .agg(  # agrega métricas por grupo
                F.round(F.sum("total_amount"), 2).alias("ingreso_total"),  # suma de ingresos, redondeado a 2 decimales
                F.round(F.avg("tip_amount"), 2).alias("propina_promedio"),  # promedio de propinas
                F.count("*").alias("total_viajes")  # conteo de viajes
            )
            .orderBy("hora"))  # ordena ascendente por hora

        kpi1_path = f"{GOLD_BASE}/kpi_financiero"  # ruta HDFS para KPI 1
        (kpi_financiero  # DataFrame del KPI
            .write  # modo escritura
            .mode("overwrite")  # sobrescribe si existe
            .option("header", "true")  # incluye encabezados de columna (para Power BI)
            .csv(kpi1_path))  # guarda como CSV
        kpi1_rows = kpi_financiero.count()  # cuenta filas generadas
        log.info("  ✓ KPI Financiero → %s (%d filas)", kpi1_path, kpi1_rows)  # log éxito

        # ──────────────────────────────────────────────────────
        # KPI 2 — OPERATIVO
        #   Duración promedio (min) y distancia promedio (km)
        #   agrupados por número de pasajeros.
        # ──────────────────────────────────────────────────────
        log.info("Calculando KPI 2 — Operativo (por passenger_count)…")  # log inicio KPI 2
        kpi_operativo = (df_silver  # sobre silver
            .groupBy("passenger_count")  # agrupa por número de pasajeros
            .agg(  # agrega métricas por grupo
                F.round(F.avg("duracion_minutos"), 2).alias("duracion_promedio_min"),  # duración promedio del viaje
                F.round(F.avg("trip_distance"), 2).alias("distancia_promedio_km"),  # distancia promedio
                F.count("*").alias("total_viajes")  # conteo de viajes
            )
            .orderBy("passenger_count"))  # ordena ascendente por pasajeros

        kpi2_path = f"{GOLD_BASE}/kpi_operativo"  # ruta HDFS para KPI 2
        (kpi_operativo  # DataFrame del KPI
            .write  # modo escritura
            .mode("overwrite")  # sobrescribe si existe
            .option("header", "true")  # incluye encabezados
            .csv(kpi2_path))  # guarda como CSV
        kpi2_rows = kpi_operativo.count()  # cuenta filas generadas
        log.info("  ✓ KPI Operativo → %s (%d filas)", kpi2_path, kpi2_rows)  # log éxito

        # ──────────────────────────────────────────────────────
        # KPI 3 — DEMANDA
        #   Total de viajes agrupados por PULocationID
        #   en orden descendente (zonas con más demanda primero).
        # ──────────────────────────────────────────────────────
        log.info("Calculando KPI 3 — Demanda (por zona de recogida)…")  # log inicio KPI 3
        kpi_demanda = (df_silver  # sobre silver
            .groupBy("PULocationID")  # agrupa por zona de recogida
            .agg(F.count("*").alias("total_viajes"))  # cuenta viajes por zona
            .orderBy(F.desc("total_viajes")))  # ordena descendente por total (más demandadas primero)

        kpi3_path = f"{GOLD_BASE}/kpi_demanda"  # ruta HDFS para KPI 3
        (kpi_demanda  # DataFrame del KPI
            .write  # modo escritura
            .mode("overwrite")  # sobrescribe si existe
            .option("header", "true")  # incluye encabezados
            .csv(kpi3_path))  # guarda como CSV
        kpi3_rows = kpi_demanda.count()  # cuenta filas generadas
        log.info("  ✓ KPI Demanda → %s (%d filas)", kpi3_path, kpi3_rows)  # log éxito

        log.info("✓ ETAPA 3 completada. 3 KPIs exportados como CSV:")  # resumen final
        log.info("    1. Financiero : %d filas", kpi1_rows)  # filas KPI 1
        log.info("    2. Operativo  : %d filas", kpi2_rows)  # filas KPI 2
        log.info("    3. Demanda    : %d filas", kpi3_rows)  # filas KPI 3
        return True  # retorna True, todo exitoso

    except Exception as e:  # captura cualquier error
        log.exception("✗ ETAPA 3 falló: %s", e)  # loggea error con traceback
        return False  # retorna False


# ═══════════════════════════════════════════════════════════════
# 5. ORQUESTADOR PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def main() -> int:  # función principal que orquesta todo el pipeline
    """
    Orquesta la ejecución completa del pipeline:

        1. Verificar entorno (PySpark)
        2. Crear SparkSession (YARN + ZeroTier)
        3. ETAPA 2: Bronze → Silver
        4. ETAPA 3: Silver → Gold (KPIs CSV)
        5. Cerrar SparkSession

    Returns
    -------
    int
        0 = éxito,
        1 = fallo en verificación de entorno,
        2 = fallo en ETAPA 2 (Silver),
        3 = fallo en ETAPA 3 (Gold),
        4 = error crítico inesperado.
    """
    log.info("═══════════════════════════════════════════════════")  # separador visual
    log.info("  PIPELINE LAKEHOUSE — Silver & Gold sobre YARN")  # título del pipeline
    log.info("  Cluster: 4 nodos ZeroTier | Driver: leo")  # info del cluster
    log.info("═══════════════════════════════════════════════════")  # separador visual

    if not verificar_entorno():  # paso 1: verifica que PySpark esté instalado
        log.critical("Abortando: PySpark no disponible.")  # log crítico
        return 1  # código 1: entorno no listo

    spark = None  # inicializa spark como None (para el finally)
    try:  # bloque try principal
        spark = crear_spark()  # paso 2: crea la SparkSession

        df_silver = etapa2_silver(spark)  # paso 3: ejecuta etapa silver
        if df_silver is None:  # si silver falló o no produjo datos
            log.critical("Abortando: ETAPA 2 (Silver) no produjo datos.")  # log crítico
            return 2  # código 2: fallo en silver

        if not etapa3_gold(spark, df_silver):  # paso 4: ejecuta etapa gold
            log.critical("Abortando: ETAPA 3 (Gold) falló.")  # log crítico
            return 3  # código 3: fallo en gold

        log.info("═══════════════════════════════════════════════════")  # separador éxito
        log.info("  ✓ PIPELINE COMPLETO: Bronze → Silver → Gold")  # mensaje éxito
        log.info("  KPIs listos para Power BI en: %s", GOLD_BASE)  # ruta de los KPIs
        log.info("═══════════════════════════════════════════════════")  # separador final
        return 0  # código 0: éxito

    except Exception as e:  # captura cualquier error no manejado antes
        log.exception("✗ Error crítico e inesperado en el pipeline: %s", e)  # log crítico con traceback
        return 4  # código 4: error inesperado

    finally:  # bloque finally: se ejecuta siempre (haya error o no)
        if spark is not None:  # si la SparkSession fue creada
            spark.stop()  # detiene la SparkSession (libera recursos en YARN)
            log.info("SparkSession cerrada correctamente.")  # confirma cierre


if __name__ == "__main__":  # si el script se ejecuta directamente
    sys.exit(main())  # llama a main() y sale con su código de retorno