#!/usr/bin/env python3
"""
ETAPA 2 (Silver) + ETAPA 3 (Gold) — Pipeline PySpark sobre YARN
═══════════════════════════════════════════════════════════════
Cluster ZeroTier: 1 Driver (leo) + 3 Workers (XUBUNTU, DEBIAN, isait-VirtualBox)
Ejecución: spark-submit --master yarn --deploy-mode client procesar_lakehouse.py
"""

import sys
import logging
from typing import Optional, Tuple

# ─── Configuración del cluster ZeroTier ───
NAMENODE_RPC = "hdfs://10.61.61.105:9000"
BRONZE_PATH = f"{NAMENODE_RPC}/lakehouse/bronze/yellow_tripdata_2023-01.parquet"
SILVER_PATH = f"{NAMENODE_RPC}/lakehouse/silver/taxis_limpio"
GOLD_BASE = f"{NAMENODE_RPC}/lakehouse/gold"

# Formato de log estructurado
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("lakehouse-spark")


# ═══════════════════════════════════════════════════════════════
# 1. VERIFICACIÓN DE ENTORNO
# ═══════════════════════════════════════════════════════════════

def verificar_entorno() -> bool:
    """
    Valida que PySpark esté disponible antes de instanciar la SparkSession.
    Si falta, emite un mensaje con el comando de instalación y retorna False.
    """
    try:
        import pyspark
        log.info("✓ PySpark %s importado correctamente", pyspark.__version__)
        return True
    except ImportError:
        log.error("✗ PySpark no encontrado en el entorno.")
        log.error("  Instálalo con: pip install pyspark")
        return False


# ═══════════════════════════════════════════════════════════════
# 2. SPARKSESSION OPTIMIZADA PARA ZEROTIER + YARN
# ═══════════════════════════════════════════════════════════════

def crear_spark() -> "SparkSession":
    """
    Crea una SparkSession configurada para el cluster ZeroTier.
    - Driver en leo (10.61.61.105), sin trabajo pesado local.
    - 3 Workers × 4 GB RAM + 2 cores.
    - Serialización Kryo, AQE, timeouts ajustados a la VPN.
    """
    from pyspark.sql import SparkSession

    log.info("Construyendo SparkSession para YARN sobre ZeroTier…")

    spark = (SparkSession.builder
        .appName("Lakehouse-Silver-Gold-ZeroTier")
        .master("yarn")

        # ── Conectividad ZeroTier ──
        .config("spark.yarn.access.namenodes", NAMENODE_RPC)
        .config("spark.driver.host", "10.61.61.105")
        .config("spark.driver.bindAddress", "10.61.61.105")

        # ── Event Log para History Server (:18080) ──
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")

        # ── Recursos: 3 workers × (4 GB + 2 cores) ──
        .config("spark.executor.instances", "3")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memoryOverhead", "1g")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.cores", "1")

        # ── Optimizaciones de ejecución ──
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.default.parallelism", "12")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")

        # ── Timeouts holgados para red ZeroTier ──
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")

        .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    log.info("✓ SparkSession activa — Driver: 10.61.61.105 | Workers: 3 × (4 GB, 2 cores)")
    log.info("  Shuffle partitions: 12 | Serializador: Kryo | AQE: habilitado")

    return spark


# ═══════════════════════════════════════════════════════════════
# 3. ETAPA 2 — CAPA PLATA (Limpieza distribuida)
# ═══════════════════════════════════════════════════════════════

def etapa2_silver(spark: "SparkSession") -> Optional["DataFrame"]:
    """
    Lee el Parquet crudo desde Bronze, aplica reglas de calidad,
    y escribe la capa Silver particionada por PULocationID.

    Returns
    -------
    DataFrame | None
        DataFrame limpio si todo fue exitoso; None en caso de error.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import DataFrame

    log.info("═══ ETAPA 2: BRONZE → SILVER ═══")
    log.info("Origen:  %s", BRONZE_PATH)
    log.info("Destino: %s", SILVER_PATH)

    try:
        # ── Lectura ──
        df = spark.read.parquet(BRONZE_PATH)
        raw_count = df.count()
        log.info("Registros leídos desde Bronze: %d", raw_count)

        if raw_count == 0:
            log.warning("Bronze retornó 0 registros. Abortando etapa Silver.")
            return None

        # ── Transformaciones de limpieza ──
        log.info("Aplicando reglas de calidad…")
        df_clean = (df
            # (a) Filtrar passenger_count y trip_distance inválidos
            .filter(
                (F.col("passenger_count") > 0) &
                (F.col("trip_distance") > 0)
            )
            # (b) Casteo explícito a Timestamp
            .withColumn("tpep_pickup_datetime",
                        F.col("tpep_pickup_datetime").cast("timestamp"))
            .withColumn("tpep_dropoff_datetime",
                        F.col("tpep_dropoff_datetime").cast("timestamp"))
            # (c) Columna calculada: duración en minutos
            .withColumn("duracion_minutos",
                        (F.unix_timestamp("tpep_dropoff_datetime") -
                         F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
            # (d) Filtrar duraciones no positivas
            .filter(F.col("duracion_minutos") > 0)
        )

        clean_count = df_clean.count()
        eliminados = raw_count - clean_count
        pct_eliminados = (eliminados * 100.0 / raw_count) if raw_count > 0 else 0.0
        log.info("Registros tras limpieza: %d (descartados: %d / %.1f%%)",
                 clean_count, eliminados, pct_eliminados)

        if clean_count == 0:
            log.warning("Tras limpieza quedan 0 registros. Abortando escritura Silver.")
            return None

        # ── Escritura Silver particionada ──
        log.info("Escribiendo capa Silver (Parquet particionado por PULocationID)…")
        (df_clean
            .write
            .mode("overwrite")
            .partitionBy("PULocationID")
            .parquet(SILVER_PATH))

        log.info("✓ ETAPA 2 completada. Silver guardado en: %s", SILVER_PATH)
        return df_clean

    except Exception as e:
        log.exception("✗ ETAPA 2 falló: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════
# 4. ETAPA 3 — CAPA ORO (KPIs para Power BI)
# ═══════════════════════════════════════════════════════════════

def etapa3_gold(spark: "SparkSession", df_silver: "DataFrame") -> bool:
    """
    Calcula 3 KPIs de negocio a partir de la capa Silver y los guarda
    como CSV con cabeceras en HDFS para consumo directo desde Power BI.

    - KPI 1 (Financiero): Ingreso total y propina promedio por hora.
    - KPI 2 (Operativo):   Duración y distancia promedio por nº de pasajeros.
    - KPI 3 (Demanda):     Total de viajes por zona de recogida (descendente).

    Parameters
    ----------
    spark : SparkSession
    df_silver : DataFrame
        DataFrame limpio proveniente de la etapa Silver.

    Returns
    -------
    bool
        True si los 3 KPIs se generaron y guardaron exitosamente.
    """
    from pyspark.sql import functions as F

    log.info("═══ ETAPA 3: SILVER → GOLD ═══")
    log.info("Destino base: %s", GOLD_BASE)

    try:
        # ──────────────────────────────────────────────────────
        # KPI 1 — FINANCIERO
        #   Ingreso total (total_amount) y propina promedio (tip_amount)
        #   agrupados por hora del día de recogida.
        # ──────────────────────────────────────────────────────
        log.info("Calculando KPI 1 — Financiero (por hora del día)…")
        kpi_financiero = (df_silver
            .withColumn("hora", F.hour("tpep_pickup_datetime"))
            .groupBy("hora")
            .agg(
                F.round(F.sum("total_amount"), 2).alias("ingreso_total"),
                F.round(F.avg("tip_amount"), 2).alias("propina_promedio"),
                F.count("*").alias("total_viajes")
            )
            .orderBy("hora"))

        kpi1_path = f"{GOLD_BASE}/kpi_financiero"
        (kpi_financiero
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(kpi1_path))
        kpi1_rows = kpi_financiero.count()
        log.info("  ✓ KPI Financiero → %s (%d filas)", kpi1_path, kpi1_rows)

        # ──────────────────────────────────────────────────────
        # KPI 2 — OPERATIVO
        #   Duración promedio (min) y distancia promedio (km)
        #   agrupados por número de pasajeros.
        # ──────────────────────────────────────────────────────
        log.info("Calculando KPI 2 — Operativo (por passenger_count)…")
        kpi_operativo = (df_silver
            .groupBy("passenger_count")
            .agg(
                F.round(F.avg("duracion_minutos"), 2).alias("duracion_promedio_min"),
                F.round(F.avg("trip_distance"), 2).alias("distancia_promedio_km"),
                F.count("*").alias("total_viajes")
            )
            .orderBy("passenger_count"))

        kpi2_path = f"{GOLD_BASE}/kpi_operativo"
        (kpi_operativo
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(kpi2_path))
        kpi2_rows = kpi_operativo.count()
        log.info("  ✓ KPI Operativo → %s (%d filas)", kpi2_path, kpi2_rows)

        # ──────────────────────────────────────────────────────
        # KPI 3 — DEMANDA
        #   Total de viajes agrupados por PULocationID
        #   en orden descendente (zonas con más demanda primero).
        # ──────────────────────────────────────────────────────
        log.info("Calculando KPI 3 — Demanda (por zona de recogida)…")
        kpi_demanda = (df_silver
            .groupBy("PULocationID")
            .agg(F.count("*").alias("total_viajes"))
            .orderBy(F.desc("total_viajes")))

        kpi3_path = f"{GOLD_BASE}/kpi_demanda"
        (kpi_demanda
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(kpi3_path))
        kpi3_rows = kpi_demanda.count()
        log.info("  ✓ KPI Demanda → %s (%d filas)", kpi3_path, kpi3_rows)

        # ── Resumen ──
        log.info("✓ ETAPA 3 completada. 3 KPIs exportados como CSV:")
        log.info("    1. Financiero : %d filas", kpi1_rows)
        log.info("    2. Operativo  : %d filas", kpi2_rows)
        log.info("    3. Demanda    : %d filas", kpi3_rows)
        return True

    except Exception as e:
        log.exception("✗ ETAPA 3 falló: %s", e)
        return False


# ═══════════════════════════════════════════════════════════════
# 5. ORQUESTADOR PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def main() -> int:
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
    log.info("═══════════════════════════════════════════════════")
    log.info("  PIPELINE LAKEHOUSE — Silver & Gold sobre YARN")
    log.info("  Cluster: 4 nodos ZeroTier | Driver: leo")
    log.info("═══════════════════════════════════════════════════")

    # ── 1. Entorno ──
    if not verificar_entorno():
        log.critical("Abortando: PySpark no disponible.")
        return 1

    spark = None
    try:
        # ── 2. SparkSession ──
        spark = crear_spark()

        # ── 3. ETAPA 2 — Silver ──
        df_silver = etapa2_silver(spark)
        if df_silver is None:
            log.critical("Abortando: ETAPA 2 (Silver) no produjo datos.")
            return 2

        # ── 4. ETAPA 3 — Gold ──
        if not etapa3_gold(spark, df_silver):
            log.critical("Abortando: ETAPA 3 (Gold) falló.")
            return 3

        log.info("═══════════════════════════════════════════════════")
        log.info("  ✓ PIPELINE COMPLETO: Bronze → Silver → Gold")
        log.info("  KPIs listos para Power BI en: %s", GOLD_BASE)
        log.info("═══════════════════════════════════════════════════")
        return 0

    except Exception as e:
        log.exception("✗ Error crítico e inesperado en el pipeline: %s", e)
        return 4

    finally:
        if spark is not None:
            spark.stop()
            log.info("SparkSession cerrada correctamente.")


if __name__ == "__main__":
    sys.exit(main())
