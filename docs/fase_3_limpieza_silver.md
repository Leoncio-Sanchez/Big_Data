# Fase 3 — Limpieza a Capa Silver

## Objetivo

Leer los datos crudos desde Bronze, aplicar reglas de calidad y transformaciones con PySpark sobre YARN, y escribir los datos limpios en la capa Silver particionados por PULocationID.

**Script:** `/home/leo/Documentos/Big data/procesar_lakehouse.py` — Función `etapa2_silver()`

---

## 1. Transformaciones Aplicadas

| # | Operación | Expresión PySpark | Propósito |
|:-:|-----------|-------------------|-----------|
| a | Filtro | `passenger_count > 0` | Eliminar viajes sin pasajeros |
| a | Filtro | `trip_distance > 0` | Eliminar viajes sin distancia |
| b | Casteo | `col("tpep_pickup_datetime").cast("timestamp")` | Asegurar tipo timestamp |
| b | Casteo | `col("tpep_dropoff_datetime").cast("timestamp")` | Asegurar tipo timestamp |
| c | Columna nueva | `(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60.0` | Duración en minutos |
| d | Filtro | `duracion_minutos > 0` | Eliminar viajes con tiempo negativo |

### Código completo de la etapa

```python
def etapa2_silver(spark: SparkSession) -> Optional[DataFrame]:
    df = spark.read.parquet(BRONZE_PATH)
    raw_count = df.count()
    log.info("Registros leídos desde Bronze: %d", raw_count)

    df_clean = (df
        .filter((F.col("passenger_count") > 0) & (F.col("trip_distance") > 0))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("duracion_minutos",
                    (F.unix_timestamp("tpep_dropoff_datetime") -
                     F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
        .filter(F.col("duracion_minutos") > 0)
    )

    clean_count = df_clean.count()
    eliminados = raw_count - clean_count
    log.info("Registros tras limpieza: %d (descartados: %d / %.1f%%)",
             clean_count, eliminados, eliminados * 100.0 / raw_count)

    (df_clean
        .write
        .mode("overwrite")
        .partitionBy("PULocationID")
        .parquet(SILVER_PATH))

    return df_clean
```

---

## 2. SparkSession Configurada para YARN + ZeroTier

### Código de creación

```python
def crear_spark() -> SparkSession:
    spark = (SparkSession.builder
        .appName("Lakehouse-Silver-Gold-ZeroTier")
        .master("yarn")
        .config("spark.yarn.access.namenodes", NAMENODE_RPC)
        .config("spark.driver.host", "10.61.61.105")
        .config("spark.driver.bindAddress", "10.61.61.105")
        .config("spark.executor.instances", "3")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memoryOverhead", "1g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.network.timeout", "800s")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
```

### Explicación de configuraciones clave

| Config | Valor | ¿Por qué? |
|--------|:-----:|-----------|
| `spark.master` | `yarn` | Usar YARN como gestor de recursos del cluster |
| `spark.driver.host` | `10.61.61.105` | IP ZeroTier de leo para que workers contacten al driver |
| `spark.executor.instances` | `3` | 1 executor por worker (XUBUNTU, DEBIAN, isait-VB) |
| `spark.executor.memory` | `4g` | Deja 4GB libres para OS en cada worker de 8GB |
| `spark.executor.cores` | `2` | 2 tareas paralelas por executor |
| `spark.sql.shuffle.partitions` | `12` | 3 executors × 2 cores × 2 = 12 particiones |
| `spark.network.timeout` | `800s` | Tolerancia a latencia de red ZeroTier |
| `spark.sql.adaptive.enabled` | `true` | AQE: Spark reoptimiza en runtime según los datos |

### Distribución de recursos

```
MASTER (leo - 10.61.61.105)
┌───────────────────────────────────────┐
│ Spark Driver (2GB RAM, 1 core)        │
│ - Planifica el DAG de transformaciones│
│ - Negocia contenedores con YARN       │
│ - Envía tareas a los executors        │
└───────────────────────────────────────┘
         │              │              │
    ┌────▼────┐    ┌────▼────┐    ┌────▼────┐
    │Executor1│    │Executor2│    │Executor3 │
    │XUBUNTU  │    │DEBIAN   │    │isait-VB  │
    │4G/2core │    │4G/2core │    │4G/2core  │
    └─────────┘    └─────────┘    └──────────┘
```

---

## 3. Particionado por PULocationID

```python
(df_clean
    .write
    .mode("overwrite")
    .partitionBy("PULocationID")   # ← 265 zonas únicas
    .parquet(SILVER_PATH))
```

**Beneficio:** Las consultas que filtran por zona solo leen la partición relevante (predicate pushdown).

**Cardinalidad:** ~265 zonas — número manejable de particiones.

---

## 4. Bugs y Problemas Encontrados

### Bug 1: `deployMode()` no existe

```python
# ❌ Error
SparkSession.builder.master("yarn").deployMode("client")
# AttributeError: 'Builder' object has no attribute 'deployMode'

# ✅ Solución: se pasa por CLI
# spark-submit --deploy-mode client
```

### Bug 2: Variables de entorno no definidas

```bash
# ❌ Error al ejecutar spark-submit
Exception: When running with master 'yarn' either
HADOOP_CONF_DIR or YARN_CONF_DIR must be set

# ✅ Solución temporal (por comando)
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
SPARK_LOCAL_IP=10.61.61.105 \
spark-submit ...

# ✅ Solución permanente (~/.bashrc)
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_LOCAL_IP=10.61.61.105
```

### Bug 3: Spark usa IP WiFi en vez de ZeroTier

```bash
# ❌ WARN al ejecutar
WARN Utils: Your hostname, leo resolves to a loopback address: 127.0.1.1;
using 10.70.84.39 instead (on interface wlo1)

# ✅ Solución
export SPARK_LOCAL_IP=10.61.61.105
```

### Bug 4: Permission denied al escribir Silver

```bash
# ❌ Error de permisos en HDFS
Permission denied: user=leo, access=WRITE,
inode="/lakehouse":hadoop:supergroup:drwxr-xr-x

# ✅ Solución (como usuario hadoop)
sudo -u hadoop /opt/hadoop/bin/hdfs dfs -chmod -R 777 /lakehouse
```

**Causa:** El directorio `/lakehouse` fue creado por `hadoop` (vía WebHDFS en bronze_ingest.py). Spark se ejecuta como `leo`. Con permisos `755`, `leo` no puede crear subdirectorios.

---

## 5. Comando de Ejecución

```bash
cd "/home/leo/Documentos/Big data"

HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
SPARK_LOCAL_IP=10.61.61.105 \
spark-submit --master yarn --deploy-mode client procesar_lakehouse.py
```

---

## 6. Logs de Ejecución

```
2026-06-25 20:47:31 | INFO | Registros leídos desde Bronze: 3066766
2026-06-25 20:47:44 | INFO | Registros tras limpieza: 2906607 (descartados: 160159 / 5.2%)
2026-06-25 20:49:55 | INFO | ✓ ETAPA 2 completada. Silver guardado
```

---

## 7. Resultados

| Métrica | Valor |
|---------|-------|
| Registros crudos (Bronze) | 3,066,766 |
| Registros limpios (Silver) | 2,906,607 |
| Descartados | 160,159 (5.2% del total) |
| Tiempo Silver | ~2.5 min |
| Particiones | 265 (PULocationID) |

---

## 8. Estructura Resultante

```
/lakehouse/
├── bronze/
│   └── yellow_tripdata_2023-01.parquet
│
└── silver/
    └── taxis_limpio/
        ├── PULocationID=1/
        ├── PULocationID=2/
        ├── ...
        └── PULocationID=265/
```
