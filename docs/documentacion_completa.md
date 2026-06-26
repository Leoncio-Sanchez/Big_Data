# Documentación Completa del Proyecto Data Lakehouse

## Cluster Hadoop + ZeroTier + PySpark

---

## Índice

1. [Introducción](#1-introducción)
2. [Topología del Cluster](#2-topología-del-cluster)
3. [Arquitectura Data Lakehouse (Medallion)](#3-arquitectura-data-lakehouse-medallion)
4. [ETAPA 1: Ingesta a Capa Bronce](#4-etapa-1-ingesta-a-capa-bronce)
5. [ETAPA 2: Limpieza a Capa Plata](#5-etapa-2-limpieza-a-capa-plata)
6. [ETAPA 3: KPIs a Capa Oro](#6-etapa-3-kpis-a-capa-oro)
7. [Ejecución del Pipeline Completo](#7-ejecución-del-pipeline-completo)
8. [Verificación y Monitoreo](#8-verificación-y-monitoreo)
9. [Anexos](#9-anexos)

---

## 1. Introducción

Este proyecto implementa un **Data Lakehouse** sobre un cluster Hadoop real de 4 nodos interconectados mediante una red privada **ZeroTier**. El objetivo es procesar el dataset **Yellow Taxi Trips de NYC (2023-01)** a través de las 3 capas del modelo **Medallion**: Bronze (crudo), Silver (limpio) y Gold (agregado/KPIs).

### Stack tecnológico

| Componente | Versión | Rol |
|------------|---------|-----|
| Hadoop | 3.3.6 | HDFS (almacenamiento) + YARN (gestión de recursos) |
| Apache Spark | 3.5.x | Motor de procesamiento distribuido |
| Python | 3.10+ | Lenguaje de scripting |
| PySpark | 3.5.x | API Python para Spark |
| ZeroTier | - | Red privada virtual entre nodos |

---

## 2. Topología del Cluster

### 2.1 Nodos

| # | Hostname | IP ZeroTier | Rol(es) |
|---|----------|-------------|---------|
| 1 | `leo` | 10.61.61.105 | **NameNode** + **ResourceManager** + DataNode + NodeManager |
| 2 | `XUBUNTU` | 10.61.61.12 | DataNode + NodeManager |
| 3 | `DEBIAN.myguest.virtualbox.org` | 10.61.61.65 | DataNode + NodeManager |
| 4 | `isait-VirtualBox` | 10.61.61.7 | DataNode + NodeManager |

### 2.2 Puertos Clave

| Servicio | Host | Puerto | Protocolo | Propósito |
|----------|------|--------|-----------|-----------|
| NameNode RPC | leo | 9000 | Binario (TCP) | Clientes HDFS: Spark, Java, hdfs CLI |
| NameNode Web UI | leo | 9870 | HTTP | Interfaz web de HDFS |
| ResourceManager Web UI | leo | 8088 | HTTP | Interfaz web de YARN |
| ResourceManager Client RPC | leo | 8032 | Binario (TCP) | Envío de aplicaciones |
| ResourceManager Scheduler RPC | leo | 8030 | Binario (TCP) | Planificador de tareas |
| DataNode Transfer | todos | 9866 | TCP | Transferencia de datos/streaming |
| DataNode HTTP | todos | 9864 | HTTP | Info server de DataNode |

### 2.3 Recursos Disponibles

| Recurso | Valor |
|---------|-------|
| Capacidad HDFS total | 509.99 GB |
| Capacidad HDFS disponible | 141.47 GB |
| Factor de replicación | 3 |
| RAM por NodeManager | 8192 MB (8 GB) |
| Bloques corruptos | 0 |

### 2.4 Red ZeroTier

Los 4 nodos están conectados mediante una red privada ZeroTier, lo que permite:
- Comunicación directa entre nodos sin depender de IPs públicas
- Latencia controlada dentro de la red virtual
- Resolución de nombres via `/etc/hosts` o IPs directas

---

## 3. Arquitectura Data Lakehouse (Medallion)

```
┌─────────────────────────────────────────────────────────────┐
│                      CAPA BRONZE                            │
│                                                             │
│  Datos crudos, INALTERABLES, trazables                      │
│  Formato: Parquet (comprimido, columnar)                    │
│  Almacenamiento: HDFS /lakehouse/bronze/                    │
│  Herramientas: Python (requests + hdfs)                     │
│  Frecuencia: Una sola vez (batch inicial)                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      CAPA SILVER                            │
│                                                             │
│  Datos LIMPIOS y VALIDADOS                                  │
│  Transformaciones: filtros, casteos, columnas derivadas     │
│  Formato: Parquet particionado                              │
│  Almacenamiento: HDFS /lakehouse/silver/taxis_limpio/       │
│  Herramientas: Apache Spark (PySpark) sobre YARN            │
│  Frecuencia: Diaria / batch                                 │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      CAPA GOLD                              │
│                                                             │
│  Datos AGREGADOS, KPIs de negocio                           │
│  Vistas: Financiero, Operativo, Demanda                     │
│  Formato: CSV con header (para Power BI)                    │
│  Almacenamiento: HDFS /lakehouse/gold/kpi_*/                │
│  Herramientas: Apache Spark (PySpark) sobre YARN            │
│  Frecuencia: Diaria / batch                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 4. ETAPA 1: Ingesta a Capa Bronce

### 4.1 Script: `bronze_ingest.py`

**Ubicación:** `/home/leo/Documentos/Big data/bronze_ingest.py`

**Propósito:** Descargar el dataset Yellow Taxi Trips desde una URL pública y subirlo al HDFS en la capa Bronze.

### 4.2 Flujo de ejecución

```
1. Verificar entorno (librerías)
         │
         ▼
2. Descargar .parquet desde URL
   (streaming 1MB chunks, timeout 120s)
         │
         ▼
3. Subir a HDFS
   (WebHDFS via HTTP, replicación 3, bloques 128MB)
         │
         ▼
4. Limpiar archivo temporal local
```

### 4.3 Código completo

```python
#!/usr/bin/env python3
import sys
import logging
from pathlib import Path

HDFS_URI = "http://leo:9870"
BRONZE_PATH = "/lakehouse/bronze"
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
LOCAL_TMP = Path("/tmp/yellow_tripdata_2023-01.parquet")

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("bronze-ingest")

REQUIRED = {"requests": "requests", "hdfs": "hdfs"}

def verificar_entorno() -> bool:
    faltantes = []
    for mod, pkg in REQUIRED.items():
        try:
            __import__(mod)
        except ImportError:
            faltantes.append(pkg)
    if faltantes:
        log.error("Faltan dependencias: %s", ", ".join(faltantes))
        return False
    return True

def descargar_data_nube(url: str, destino: Path, timeout: int = 120) -> bool:
    import requests
    try:
        log.info("Descargando %s → %s", url, destino)
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with destino.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        return True
    except requests.RequestException as e:
        log.exception("Fallo de red: %s", e)
        return False

def subir_a_hdfs(local_file: Path, hdfs_dir: str, hdfs_uri: str) -> bool:
    from hdfs import InsecureClient
    try:
        client = InsecureClient(hdfs_uri, user="hadoop", timeout=300)
        client.makedirs(hdfs_dir, permission=0o755)
        remote_path = f"{hdfs_dir}/{local_file.name}"
        log.info("Subiendo a HDFS: %s", remote_path)
        with local_file.open("rb") as rdr:
            client.write(remote_path, rdr, overwrite=True, replication=3, blocksize=128 * 1024 * 1024)
        return True
    except Exception as e:
        log.exception("Error HDFS: %s", e)
        return False

def main() -> int:
    if not verificar_entorno(): return 1
    if LOCAL_TMP.exists(): LOCAL_TMP.unlink()
    if not descargar_data_nube(SOURCE_URL, LOCAL_TMP): return 2
    if not subir_a_hdfs(LOCAL_TMP, BRONZE_PATH, HDFS_URI): return 3
    LOCAL_TMP.unlink(missing_ok=True)
    log.info("ETAPA 1 OK")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

### 4.4 Decisiones técnicas

#### Librería `hdfs` (WebHDFS) en lugar de `hdfs3`

| Aspecto | hdfs3 | hdfs (WebHDFS) |
|---------|-------|-----------------|
| Protocolo | libhdfs3 (C++/JNI) | HTTP REST |
| Puerto | 9000 (RPC) | 9870 (HTTP) |
| Uso recomendado | Java/Spark nativo | Scripts Python externos |
| Dependencias | libhdfs3.so nativo | Solo requests HTTP |

**Decisión:** Usamos `hdfs` (WebHDFS) porque desde Python puro no tenemos el contexto JVM de Spark. El puerto `9870` (HTTP) es el correcto para clientes externos. El puerto `9000` es exclusivamente para comunicación RPC binaria (Java/Spark).

#### Dataset: Parquet en lugar de XLSX

**Dataset original:** Online Retail (Excel, ~40 MB, partículas)
**Dataset elegido:** Yellow Taxi Trips NYC 2023-01 (Parquet, ~90 MB, 20+ columnas)

**Razones del cambio:**
- Formato nativo columnar (Parquet) → procesamiento más eficiente en Spark
- Mayor volumen y complejidad → mejor demostración de capacidades distribuidas
- Datos más realistas para KPIs de negocio (financiero, operativo, demanda)

### 4.5 Ejecución

```bash
cd "/home/leo/Documentos/Big data"
pip install hdfs requests
python3 bronze_ingest.py
```

### 4.6 Verificación

```bash
hdfs dfs -ls /lakehouse/bronze/
# Resultado: /lakehouse/bronze/yellow_tripdata_2023-01.parquet (90 MB, réplica 3)

# Desde la web: http://10.61.61.105:9870 → /lakehouse/bronze/
```

---

## 5. ETAPA 2: Limpieza a Capa Plata

### 5.1 Script: `procesar_lakehouse.py` (Parte 1)

**Ubicación:** `/home/leo/Documentos/Big data/procesar_lakehouse.py`

**Función:** `etapa2_silver(spark)`

**Propósito:** Leer datos crudos de Bronze, aplicar limpieza y transformaciones, y guardar resultados limpios en Silver.

### 5.2 Transformaciones aplicadas

| Transformación | Expresión PySpark | Propósito |
|----------------|-------------------|-----------|
| Filtro pasajeros | `passenger_count > 0` | Eliminar registros sin pasajeros |
| Filtro distancia | `trip_distance > 0` | Eliminar viajes sin distancia |
| Casteo pickup | `col("tpep_pickup_datetime").cast("timestamp")` | Asegurar tipo Timestamp |
| Casteo dropoff | `col("tpep_dropoff_datetime").cast("timestamp")` | Asegurar tipo Timestamp |
| Columna duración | `(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60.0` | Duración en minutos |
| Filtro duración | `duracion_minutos > 0` | Eliminar viajes con tiempo negativo/cero |

### 5.3 Código (función Silver)

```python
def etapa2_silver(spark: SparkSession) -> Optional[DataFrame]:
    """Lee Bronze, limpia, escribe Silver particionado."""
    from pyspark.sql import functions as F

    try:
        log.info("ETAPA 2: Leyendo Bronze desde %s", BRONZE_PATH)
        df = spark.read.parquet(BRONZE_PATH)
        raw_count = df.count()
        log.info("Registros brutos leídos: %d", raw_count)

        df_clean = (df
            .filter((F.col("passenger_count") > 0) & (F.col("trip_distance") > 0))
            .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
            .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
            .withColumn("duracion_minutos",
                        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
            .filter(F.col("duracion_minutos") > 0)
        )

        clean_count = df_clean.count()
        log.info("Registros tras limpieza: %d (eliminados: %d, %.1f%%)",
                 clean_count, raw_count - clean_count, (raw_count - clean_count) * 100 / raw_count)

        (df_clean
            .write
            .mode("overwrite")
            .partitionBy("PULocationID")
            .parquet(SILVER_PATH))

        log.info("ETAPA 2 OK: Silver guardado en %s", SILVER_PATH)
        return df_clean

    except Exception as e:
        log.exception("Fallo en ETAPA 2 (Silver): %s", e)
        return None
```

### 5.4 Particionado por PULocationID

**Decisión técnica:** Particionar Silver por `PULocationID` (ID de zona de recogida).

**Beneficios:**
- **Predicate pushdown:** Consultas por zona leen solo las particiones relevantes
- **KPI 3 optimizado:** La agregación por zona ya tiene datos pre-agrupados
- **Escalabilidad:** Cada partición puede procesarse en paralelo en diferentes workers

**Contraindicaciones:**
- Cardinalidad media (~265 zonas únicas) → número manejable de particiones
- Si la cardinalidad fuera muy alta (>1000), mejor particionar por fecha

---

## 6. ETAPA 3: KPIs a Capa Oro

### 6.1 Script: `procesar_lakehouse.py` (Parte 2)

**Función:** `etapa3_gold(spark, df_silver)`

**Propósito:** Calcular 3 KPIs de negocio desde los datos limpios de Silver y guardarlos como CSV para Power BI.

### 6.2 KPI 1: Financiero - Ingreso por hora

```python
kpi1 = (df_silver
    .withColumn("hora", F.hour("tpep_pickup_datetime"))
    .groupBy("hora")
    .agg(
        F.sum("total_amount").alias("ingreso_total"),
        F.avg("tip_amount").alias("propina_promedio"),
        F.count("*").alias("total_viajes")
    )
    .orderBy("hora"))
```

**Qué responde:** ¿En qué horas del día se genera más ingreso? ¿Dónde se deja más propina?

**Salida:** `/lakehouse/gold/kpi_financiero/`

| hora | ingreso_total | propina_promedio | total_viajes |
|------|---------------|------------------|--------------|
| 0 | 850,234.50 | 3.45 | 12,450 |
| 1 | 620,100.75 | 3.12 | 9,230 |
| ... | ... | ... | ... |

### 6.3 KPI 2: Operativo - Rendimiento por pasajero

```python
kpi2 = (df_silver
    .groupBy("passenger_count")
    .agg(
        F.avg("duracion_minutos").alias("duracion_promedio_min"),
        F.avg("trip_distance").alias("distancia_promedio_km"),
        F.count("*").alias("total_viajes")
    )
    .orderBy("passenger_count"))
```

**Qué responde:** ¿Los viajes con más pasajeros son más largos? ¿Son más eficientes?

**Salida:** `/lakehouse/gold/kpi_operativo/`

| passenger_count | duracion_promedio_min | distancia_promedio_km | total_viajes |
|----------------|----------------------|----------------------|--------------|
| 1 | 15.2 | 4.8 | 450,000 |
| 2 | 18.7 | 6.1 | 120,000 |
| ... | ... | ... | ... |

### 6.4 KPI 3: Demanda - Viajes por zona

```python
kpi3 = (df_silver
    .groupBy("PULocationID")
    .agg(F.count("*").alias("total_viajes"))
    .orderBy(F.desc("total_viajes")))
```

**Qué responde:** ¿Cuáles son las zonas con mayor demanda de taxis en NYC?

**Salida:** `/lakehouse/gold/kpi_demanda/`

| PULocationID | total_viajes |
|-------------|--------------|
| 237 | 25,430 |
| 236 | 22,100 |
| ... | ... |

### 6.5 Formato CSV para Power BI

```python
(kpi1
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(kpi1_path))
```

**Decisión técnica:** CSV con header en lugar de Parquet para Gold.

| Formato | Ventaja | Desventaja |
|---------|---------|------------|
| **Parquet** | Columnar, comprimido, schema nativo | Power BI requiere config adicional |
| **CSV** | Lectura universal, Power BI lo consume directo | Más peso, sin compresión |

Power BI puede leer CSV directamente desde HDFS via WebHDFS o conectar via ODBC.

---

## 7. Ejecución del Pipeline Completo

### 7.1 SparkSession optimizada para YARN + ZeroTier

```python
def crear_spark() -> SparkSession:
    spark = (SparkSession.builder
        .appName("Lakehouse-Silver-Gold-ZeroTier")
        .master("yarn")
        .deployMode("client")
        .config("spark.yarn.access.namenodes", "hdfs://10.61.61.105:9000")
        .config("spark.driver.host", "10.61.61.105")
        .config("spark.driver.bindAddress", "10.61.61.105")
        .config("spark.executor.instances", "3")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memoryOverhead", "1g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.default.parallelism", "12")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate())
    return spark
```

### 7.2 Explicación de cada configuración

| Configuración | Valor | Razón técnica |
|---------------|-------|---------------|
| `spark.master` | `yarn` | Usar YARN como gestor de recursos del cluster |
| `spark.deployMode` | `client` | Driver corre en leo (master), executors en workers |
| `spark.yarn.access.namenodes` | `hdfs://10.61.61.105:9000` | Delega tokens HDFS a executors remotos |
| `spark.driver.host` | `10.61.61.105` | IP ZeroTier de leo para que los workers contacten al driver |
| `spark.driver.bindAddress` | `10.61.61.105` | Driver escucha en interfaz ZeroTier (no localhost) |
| `spark.executor.instances` | `3` | 1 executor por cada worker (XUBUNTU, DEBIAN, isait-VB) |
| `spark.executor.memory` | `4g` | Deja 4GB libres para OS + YARN overhead en cada worker de 8GB |
| `spark.executor.cores` | `2` | 2 tareas paralelas por executor |
| `spark.sql.shuffle.partitions` | `12` | 3 executors × 2 cores × 2 factor = 12 particiones |
| `spark.network.timeout` | `800s` | Tolerancia a latencia de red ZeroTier |
| `spark.sql.adaptive.enabled` | `true` | AQE: Spark reoptimiza en runtime según datos |

### 7.3 Diagrama de ejecución distribuida

```
MASTER (leo - 10.61.61.105)
┌─────────────────────────────────────────────┐
│  Spark Driver                               │
│  - Crea DAG de transformaciones              │
│  - Negocia recursos con ResourceManager      │
│  - Distribuye tasks a executors              │
│  - Recoge resultados                         │
└─────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ Executor 1       │  │ Executor 2       │  │ Executor 3       │
│ XUBUNTU          │  │ DEBIAN           │  │ isait-VirtualBox  │
│ 10.61.61.12      │  │ 10.61.61.65      │  │ 10.61.61.7        │
│ 4GB RAM / 2 cores│  │ 4GB RAM / 2 cores│  │ 4GB RAM / 2 cores│
│                  │  │                  │  │                  │
│ Lee particiones  │  │ Lee particiones  │  │ Lee particiones  │
│ de HDFS locales  │  │ de HDFS locales  │  │ de HDFS locales  │
│ Filtra, castea   │  │ Filtra, castea   │  │ Filtra, castea   │
│ Calcula KPIs     │  │ Calcula KPIs     │  │ Calcula KPIs     │
│ Escribe a HDFS   │  │ Escribe a HDFS   │  │ Escribe a HDFS   │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

### 7.4 Comando de ejecución

```bash
cd "/home/leo/Documentos/Big data"

spark-submit --master yarn \
  --deploy-mode client \
  procesar_lakehouse.py
```

**Nota importante:** Los parámetros del executor (`--num-executors`, `--executor-memory`, `--executor-cores`) están hardcodeados en el código mediante `.config()`. No es necesario pasarlos por CLI, pero si se pasan, tienen prioridad.

### 7.5 Logs esperados

```
2026-06-25 18:30:01 | INFO     | PySpark 3.5.x disponible
2026-06-25 18:30:01 | INFO     | SparkSession creada - Master YARN, Driver: 10.61.61.105, 3 Executors × 4GB/2 cores
2026-06-25 18:30:02 | INFO     | ETAPA 2: Leyendo Bronze desde hdfs://10.61.61.105:9000/lakehouse/bronze/yellow_tripdata_2023-01.parquet
2026-06-25 18:30:10 | INFO     | Registros brutos leídos: 3,006,764
2026-06-25 18:30:45 | INFO     | Registros tras limpieza: 2,890,123 (eliminados: 116,641, 3.9%)
2026-06-25 18:30:50 | INFO     | ETAPA 2 OK: Silver guardado en hdfs://10.61.61.105:9000/lakehouse/silver/taxis_limpio
2026-06-25 18:30:50 | INFO     | ETAPA 3: Calculando KPIs desde Silver
2026-06-25 18:31:20 | INFO     | KPI 1 (Financiero) guardado en .../gold/kpi_financiero (24 filas)
2026-06-25 18:31:25 | INFO     | KPI 2 (Operativo) guardado en .../gold/kpi_operativo (8 filas)
2026-06-25 18:31:35 | INFO     | KPI 3 (Demanda) guardado en .../gold/kpi_demanda (265 filas)
2026-06-25 18:31:35 | INFO     | PIPELINE COMPLETO: Bronze -> Silver -> Gold (CSV para Power BI)
```

---

## 8. Verificación y Monitoreo

### 8.1 Interfaces Web

| Servicio | URL | Para qué |
|----------|-----|----------|
| NameNode HDFS | http://10.61.61.105:9870 | Explorar archivos en HDFS, ver DataNodes vivos |
| ResourceManager YARN | http://10.61.61.105:8088 | Ver aplicaciones Spark, executors, logs |
| DataNode leo | http://10.61.61.105:9864 | Estado del DataNode local |
| DataNode XUBUNTU | http://10.61.61.12:9864 | Estado del DataNode worker |
| DataNode DEBIAN | http://10.61.61.65:9864 | Estado del DataNode worker |
| NodeManager leo | http://10.61.61.105:8042 | Estado del NodeManager local |
| Spark History Server | http://10.61.61.105:18080 | Historial de jobs Spark (si está activo) |

### 8.2 Comandos de verificación

```bash
# Ver estructura HDFS completa
hdfs dfs -ls -R /lakehouse/

# Ver archivos en Bronze
hdfs dfs -ls /lakehouse/bronze/

# Ver archivos en Silver
hdfs dfs -ls /lakehouse/silver/taxis_limpio/

# Ver KPIs Gold
hdfs dfs -ls /lakehouse/gold/kpi_financiero/
hdfs dfs -ls /lakehouse/gold/kpi_operativo/
hdfs dfs -ls /lakehouse/gold/kpi_demanda/

# Leer CSV de Gold
hdfs dfs -cat /lakehouse/gold/kpi_financiero/*.csv | head
hdfs dfs -cat /lakehouse/gold/kpi_operativo/*.csv | head
hdfs dfs -cat /lakehouse/gold/kpi_demanda/*.csv | head

# Reporte HDFS
hdfs dfsadmin -report

# Listar nodos YARN
yarn node -list

# Ver aplicación en YARN
yarn application -list
yarn application -status application_XXXX

# Ver logs de aplicación
yarn logs -applicationId application_XXXX
```

### 8.3 Monitoreo en Spark UI (puerto 4040 efímero)

Durante la ejecución del job Spark, la UI está disponible en:
- **Jobs:** http://10.61.61.105:4040/jobs/
- **Stages:** http://10.61.61.105:4040/stages/
- **Executors:** http://10.61.61.105:4040/executors/
- **SQL:** http://10.61.61.105:4040/SQL/

Para jobs terminados, usar History Server: http://10.61.61.105:18080/

### 8.4 Interpretación de métricas en Spark UI

| Métrica | Qué indica | Valor saludable |
|---------|------------|-----------------|
| **Shuffle Read/Write** | Datos movidos entre workers | < 10 GB |
| **GC Time** | Tiempo en garbage collection | < 5% del total |
| **Scheduler Delay** | Tiempo de planificación | < 500ms |
| **Task Deserialization** | Tiempo deserializando tareas | < 100ms |
| **Result Serialization** | Tiempo serializando resultados | < 100ms |
| **Getting Result Time** | Tiempo enviando resultado al driver | < 1s |

---

## 9. Anexos

### 9.1 Estructura completa del proyecto

```
/home/leo/Documentos/Big data/
├── bronze_ingest.py              # ETAPA 1: Ingesta a Bronce
├── procesar_lakehouse.py          # ETAPA 2+3: Silver + Gold (PySpark/YARN)
└── docs/
    └── documentacion_completa.md  # Este archivo
```

### 9.2 Dataset: Yellow Taxi Trips NYC 2023-01

| Propiedad | Valor |
|-----------|-------|
| Fuente | NYC Taxi & Limousine Commission |
| URL | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet |
| Formato | Parquet |
| Tamaño | ~90 MB comprimido |
| Filas | ~3 millones |
| Columnas principales | `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `total_amount`, `tip_amount`, `PULocationID`, `DOLocationID` |

### 9.3 Esquema de columnas (Bronze -> Silver)

| Columna | Tipo original | Tipo Silver | Transformación |
|---------|--------------|-------------|----------------|
| VendorID | long | long | - |
| tpep_pickup_datetime | string | **timestamp** | cast |
| tpep_dropoff_datetime | string | **timestamp** | cast |
| passenger_count | long | long | filtro > 0 |
| trip_distance | double | double | filtro > 0 |
| RatecodeID | long | long | - |
| store_and_fwd_flag | string | string | - |
| PULocationID | long | long | - (usado para particionar) |
| DOLocationID | long | long | - |
| payment_type | long | long | - |
| fare_amount | double | double | - |
| extra | double | double | - |
| mta_tax | double | double | - |
| tip_amount | double | double | - |
| tolls_amount | double | double | - |
| improvement_surcharge | double | double | - |
| total_amount | double | double | - |
| congestion_surcharge | double | double | - |
| airport_fee | double | double | - |
| **duracion_minutos** | - | **double** | **(dropoff - pickup) / 60** |

### 9.4 Librerías utilizadas

| Librería | Versión | Instalación | Propósito |
|----------|---------|-------------|-----------|
| pyspark | 3.5.x | `pip install pyspark` | API Python de Spark |
| hdfs | - | `pip install hdfs` | Cliente WebHDFS para Python puro |
| requests | - | `pip install requests` | Descarga HTTP de archivos |

### 9.5 Troubleshooting común

| Problema | Síntoma | Causa | Solución |
|----------|---------|-------|----------|
| Driver no accesible | Executors fallan con "Connection refused" | `spark.driver.host` apunta a localhost | Configurar `spark.driver.host` con IP ZeroTier |
| HDFS no encontrado | "File does not exist" en Spark | Usar puerto 9870 en lugar de 9000 para Python puro | Spark usa RPC (9000), Python usa WebHDFS (9870) |
| Executor OOM | Executor muere con "Java heap space" | Memoria insuficiente para shuffle | Aumentar `spark.executor.memory` o reducir particiones |
| Tiempo de red | Tasks fallan con timeout | Latencia ZeroTier > default 120s | Aumentar `spark.network.timeout` a 800s |
| Shuffle lento | Stages de shuffle muy lentos | Muchas particiones pequeñas | Reducir `spark.sql.shuffle.partitions` |

### 9.6 Mejores prácticas aplicadas

1. **Data Lakehouse (Medallion):** Separación clara en 3 capas con datos inmutables
2. **Data locality:** Spark lee datos donde están almacenados (HDFS local a cada worker)
3. **Particionado estratégico:** Silver particionado por PULocationID para optimizar consultas
4. **AQE (Adaptive Query Execution):** Spark reoptimiza en runtime
5. **Kryo Serializer:** Mejor rendimiento que Java Serializer por defecto
6. **Streaming en descarga:** Chunks de 1MB para no saturar RAM
7. **Logging estructurado:** Registro temporal de cada etapa con conteos
8. **Replicación 3x:** Tolerancia a fallos en HDFS

---

*Documentación generada el 25 de Junio de 2026*
*Cluster: Hadoop 3.3.6 | Spark 3.5.x | ZeroTier | 4 nodos (1 Master + 3 Workers)*
