# Fase 4 — KPIs a Capa Gold

## Objetivo

Calcular 3 KPIs de negocio desde los datos limpios en Silver y exportarlos como CSV con cabecera para consumo directo desde Power BI.

**Script:** `/home/leo/Documentos/Big data/procesar_lakehouse.py` — Funciones `etapa3_gold()` y `main()`

---

## 1. Verificación del estado del cluster

Antes de ejecutar el pipeline, verificar que HDFS y YARN estén operativos:

### 1.1 Procesos Java activos (jps)

```bash
jps
```

Salida esperada (en el master):

```
12345 NameNode
12346 DataNode
12347 SecondaryNameNode
12348 ResourceManager
12349 NodeManager
```

Si faltan procesos, iniciar servicios:

```bash
# HDFS
/opt/hadoop/sbin/start-dfs.sh

# YARN
/opt/hadoop/sbin/start-yarn.sh
```

### 1.2 Nodos de YARN

```bash
yarn node -list
```

Salida esperada — nodos en estado `RUNNING`:

```
2026-06-25 20:00:00,000 INFO ...: Connecting to ResourceManager at /10.61.61.105:8032
Total Nodes:3
         Node-Id             Node-State Node-Http-Address       Number-of-Running-Containers
   xubuntu:45123              RUNNING    xubuntu:8042            0
   debian:45124               RUNNING    debian:8042             0
   isait-VirtualBox:45125     RUNNING    isait-VirtualBox:8042   0
```

### 1.3 Aplicaciones en YARN

```bash
yarn application -list
```

Muestra las aplicaciones Spark activas o finalizadas.

### 1.4 Estado de HDFS

```bash
hdfs dfsadmin -report 2>/dev/null | head -20
```

Salida esperada:

```
Configured Capacity: 529 GB
Present Capacity: 480 GB
DFS Remaining: 450 GB
DFS Used: 30 GB
DFS Used%: 6.25%
Live datanodes: 3
Dead datanodes: 0
```

### 1.5 Datos existentes en el lakehouse

```bash
hdfs dfs -ls -R /lakehouse/ 2>/dev/null
```

Verifica que las capas Bronze/Silver/Gold tengan datos antes de ejecutar.

---

## 2. Pipeline Completo — Bronze → Silver → Gold

### 2.1 Arquitectura del pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│                      PIPELINE DATA LAKEHOUSE                     │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │  BRONZE  │ →  │  SILVER  │ →  │   GOLD   │ →  │ DASHBOARD│  │
│  │          │    │          │    │          │    │          │  │
│  │ raw data │    │  clean   │    │   KPIs   │    │Streamlit │  │
│  │ Parquet  │    │ Parquet  │    │   CSV    │    │  Plotly  │  │
│  └────┬─────┘    └────┬─────┘    └────┬─────┘    └──────────┘  │
│       │               │               │                         │
│       ▼               ▼               ▼                         │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                   HDFS — /lakehouse/                      │   │
│  │  leo:9870 (Web UI) · 10.61.61.105:9000 (RPC)             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Motor: PySpark 3.5 sobre YARN · 3 executors × (2GB + 3 cores)  │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 Orquestación: función `main()`

El pipeline completo se ejecuta desde un solo script. La función `main()` en `procesar_lakehouse.py` orquesta cada etapa secuencialmente:

```python
def main() -> int:
    if not verificar_entorno():       # Paso 1: PySpark instalado?
        return 1

    spark = None
    try:
        spark = crear_spark()         # Paso 2: SparkSession en YARN

        df_silver = etapa2_silver(spark)   # Paso 3: Bronze → Silver
        if df_silver is None:
            return 2

        if not etapa3_gold(spark, df_silver):  # Paso 4: Silver → Gold
            return 3

        return 0  # ✅ Éxito

    finally:
        if spark is not None:
            spark.stop()              # Paso 5: Liberar recursos en YARN
```

**Códigos de retorno:**

| Código | Significado |
|:------:|-------------|
| 0 | Pipeline completado con éxito ✅ |
| 1 | PySpark no está instalado |
| 2 | ETAPA 2 (Silver) falló |
| 3 | ETAPA 3 (Gold) falló |
| 4 | Error crítico inesperado |

### 2.3 Mapa de ejecución distribuida

```
MASTER — leo (10.61.61.105)
┌─────────────────────────────────────────────┐
│  Spark Driver (2GB, 1 core)                 │
│  · Crea DAG de transformaciones              │
│  · Negocia contenedores con ResourceManager  │
│  · Distribuye tasks entre executors          │
│  · Recoge resultados parciales               │
└──────────┬──────────────┬──────────┬────────┘
           │              │          │
      ┌────▼────┐   ┌────▼────┐  ┌──▼────────┐
      │Executor1│   │Executor2│  │ Executor3  │
      │XUBUNTU  │   │DEBIAN   │  │ isait-VB   │
      │2G/3core │   │2G/3core │  │ 2G/3core   │
      │         │   │         │  │            │
      │Lee/HDFS │   │Lee/HDFS │  │ Lee/HDFS   │
      │Filtra   │   │Filtra   │  │ Filtra     │
      │Agrega   │   │Agrega   │  │ Agrega     │
      └─────────┘   └─────────┘  └────────────┘
```

### 2.4 Trazado completo de ejecución (con tiempos reales)

```
ETAPA                         | TIEMPO    | RESULTADO
══════════════════════════════╪═══════════╪══════════════════════════════════════
1. Verificación PySpark       | instant.  | ✓ PySpark 3.5.0 disponible
2. Creación SparkSession      | ~2 min    | ✓ YARN, 3 executors × 2GB/3 cores
   ├─ Upload spark_libs.zip   |           |   Subida de JARs a HDFS staging
   ├─ Upload pyspark.zip      |           |
   └─ Submit to ResourceManager|          |   application_...0005 ACCEPTED→RUNNING
3. ETAPA 2: Lectura Bronze    | ~8s       |   3,066,766 registros leídos ✅
4. ETAPA 2: Limpieza          | ~13s      |   2,906,607 registros (5.2% descarte) ✅
5. ETAPA 2: Escritura Silver  | ~2 min    |   Parquet particionado por PULocationID ✅
6. ETAPA 3: KPI Financiero    | ~29s      |   24 filas (ingreso/propina por hora) ✅
7. ETAPA 3: KPI Operativo     | ~14s      |   8 filas (duración/dist por pasajeros) ✅
8. ETAPA 3: KPI Demanda       | ~26s      |   254 filas (viajes por zona) ✅
9. SparkSession cerrada       | instant.  |   Recursos liberados en YARN ✅
──────────────────────────────┴───────────┴──────────────────────────────────────
   TIEMPO TOTAL               | ~3.5 min  |   ✅ PIPELINE COMPLETO
```

### 2.5 Comando de ejecución

```bash
cd "/home/leo/Documentos/Big data"

HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
SPARK_LOCAL_IP=10.61.61.105 \
spark-submit --master yarn --deploy-mode client procesar_lakehouse.py
```

### 2.6 Logs reales de la ejecución

```
2026-06-25 20:47:31 | lakehouse-spark       | Registros leídos desde Bronze: 3066766
2026-06-25 20:47:44 | lakehouse-spark       | Registros tras limpieza: 2906607 (descartados: 160159 / 5.2%)
2026-06-25 20:49:55 | lakehouse-spark       | ✓ ETAPA 2 completada. Silver guardado
2026-06-25 20:50:24 | lakehouse-spark       |   ✓ KPI Financiero → .../gold/kpi_financiero (24 filas)
2026-06-25 20:50:38 | lakehouse-spark       |   ✓ KPI Operativo → .../gold/kpi_operativo (8 filas)
2026-06-25 20:51:04 | lakehouse-spark       |   ✓ KPI Demanda → .../gold/kpi_demanda (254 filas)
2026-06-25 20:51:04 | lakehouse-spark       | ✓ PIPELINE COMPLETO: Bronze → Silver → Gold
```

### 2.7 Estados de la aplicación en YARN

| Momento | Estado | Contenedores | Descripción |
|---------|:------:|:------------:|-------------|
| `spark-submit` enviado | `ACCEPTED` | 0 | En cola del ResourceManager |
| AM asignado | `RUNNING` | 1 (AM) | ApplicationMaster negociando recursos |
| Executors desplegados | `RUNNING` | 4 (AM + 3 exec) | Workers listos para procesar |
| Transformaciones activas | `RUNNING` | 4 | Procesando datos en paralelo |
| `spark.stop()` ejecutado | `FINISHED` | 0 | Recursos liberados |

---

## 3. KPI 1 — Financiero: Ingreso y propina por hora

### Consulta PySpark

```python
kpi_financiero = (df_silver
    .withColumn("hora", F.hour("tpep_pickup_datetime"))
    .groupBy("hora")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("ingreso_total"),
        F.round(F.avg("tip_amount"), 2).alias("propina_promedio"),
        F.count("*").alias("total_viajes")
    )
    .orderBy("hora"))
```

### ¿Qué responde?

- **¿En qué horas se genera más ingreso?** — Horas pico de facturación
- **¿Dónde se deja más propina?** — Horas con mejor propina promedio
- **¿Cuál es el volumen de viajes por hora?** — Distribución horaria

### Salida

| Ruta | Formato | Filas |
|------|---------|:-----:|
| `/lakehouse/gold/kpi_financiero/` | CSV con header | 24 (0-23) |

| hora | ingreso_total | propina_promedio | total_viajes |
|:----:|:-------------:|:----------------:|:------------:|
| 0 | $850,234.50 | $3.45 | 12,450 |
| 1 | $620,100.75 | $3.12 | 9,230 |
| ... | ... | ... | ... |
| 17 | $5,620,315.00 | $3.83 | 205,034 |

---

## 4. KPI 2 — Operativo: Rendimiento por pasajero

### Consulta PySpark

```python
kpi_operativo = (df_silver
    .groupBy("passenger_count")
    .agg(
        F.round(F.avg("duracion_minutos"), 2).alias("duracion_promedio_min"),
        F.round(F.avg("trip_distance"), 2).alias("distancia_promedio_km"),
        F.count("*").alias("total_viajes")
    )
    .orderBy("passenger_count"))
```

### ¿Qué responde?

- **¿Viajes con más pasajeros son más largos?** — Correlación pasajeros vs duración
- **¿Son más eficientes?** — Distancia promedio por pasajero
- **¿Qué cantidad de pasajeros es más común?** — Distribución de viajes

### Salida

| Ruta | Formato | Filas |
|------|---------|:-----:|
| `/lakehouse/gold/kpi_operativo/` | CSV con header | 8 (1-8 pasajeros) |

| passenger_count | duracion_promedio_min | distancia_promedio_km | total_viajes |
|:---------------:|:---------------------:|:---------------------:|:------------:|
| 1 | 15.2 min | 4.8 km | 1,850,000 |
| 2 | 18.7 min | 6.1 km | 820,000 |
| 3 | 22.1 min | 7.3 km | 180,000 |

---

## 5. KPI 3 — Demanda: Viajes por zona de recogida

### Consulta PySpark

```python
kpi_demanda = (df_silver
    .groupBy("PULocationID")
    .agg(F.count("*").alias("total_viajes"))
    .orderBy(F.desc("total_viajes")))
```

### ¿Qué responde?

- **¿Zonas con mayor demanda?** — Ranking de PULocationID
- **¿Concentración del Top N?** — Porcentaje en zonas populares

### Salida

| Ruta | Formato | Filas |
|------|---------|:-----:|
| `/lakehouse/gold/kpi_demanda/` | CSV con header | 254 |

| PULocationID | total_viajes |
|:------------:|:------------:|
| 237 | 25,430 |
| 236 | 22,100 |
| 161 | 18,750 |

---

## 6. Formato de Exportación

```python
(kpi.write
    .mode("overwrite")
    .option("header", "true")
    .csv(kpi_path))
```

### CSV vs Parquet para Power BI

| Formato | Ventaja | Desventaja |
|---------|---------|------------|
| **Parquet** | Columnar, comprimido, schema nativo | Power BI requiere config extra |
| **CSV** | Universal, Power BI directo | Más peso, sin compresión |

---

## 7. Resultados Finales

| Indicador | Valor |
|-----------|-------|
| Registros crudos (Bronze) | 3,066,766 |
| Registros limpios (Silver) | 2,906,607 |
| Registros descartados | 160,159 (5.2%) |
| Tamaño Bronze | 45.5 MB (×3 réplicas = 136.4 MB) |
| Tiempo total del pipeline | ~3.5 minutos |
| Workers utilizados | 3 × (4 GB RAM, 3 CPUs) |
| Shuffle partitions | 18 |
| KPI Financiero | 24 filas |
| KPI Operativo | 8 filas |
| KPI Demanda | 254 filas |

---

## 8. Estructura Final en HDFS

```
/lakehouse/
├── bronze/
│   └── yellow_tripdata_2023-01.parquet          45.5 MB × réplica 3
│
├── silver/
│   └── taxis_limpio/
│       ├── PULocationID=1/        (partición)
│       ├── PULocationID=2/
│       ├── ...
│       └── PULocationID=265/
│
└── gold/
    ├── kpi_financiero/
    │   ├── part-00000-....csv      (24 filas con header)
    │   └── ...
    ├── kpi_operativo/
    │   ├── part-00000-....csv      (8 filas con header)
    │   └── ...
    └── kpi_demanda/
        ├── part-00000-....csv      (254 filas con header)
        └── ...
```
