# 🏛️ Data Lakehouse — NYC Yellow Taxi Trip Analytics

Pipeline completo **Bronze → Silver → Gold** sobre un cluster **Hadoop 3.3.6 + Spark 3.5 + ZeroTier** de 4 nodos. Procesa ~3 millones de viajes de taxi de NYC y genera KPIs de negocio visualizados en un dashboard interactivo.

---

## 🏗️ Arquitectura

Se adoptó el modelo **Data Lakehouse** con patrón Medallion (Bronze → Silver → Gold) porque el dataset NYC Taxi es semiestructurado (Parquet, fechas como strings, nulos). Un Data Warehouse exigiría schema rígido antes de cargar; un Data Lake dejaría los datos crudos sin estructura de consumo. El Lakehouse resuelve ambos: ingesta sin filtro (Bronze), calidad y tipado (Silver), y KPIs de negocio listos para Power BI (Gold).

El **procesamiento es batch** (no streaming) porque el dataset es histórico mensual, no un flujo continuo. **PySpark sobre YARN** distribuye las transformaciones en 3 executors (4GB/2 cores cada uno) en workers separados, logrando ~3.5 min para 3M registros — inviable en un solo nodo. **HDFS** unifica el almacenamiento con replicación 3 y ZeroTier actúa como SD-WAN para conectar nodos en redes físicas distintas bajo el rango `10.61.61.x`.

```
Bronze (Parquet crudo, 45 MB)  →  Silver (Parquet limpio, 2.9M rows)  →  Gold (CSV, 286 filas)
       │                              │                                      │
  Ingesta Python puro           Spark en YARN (3 executors)          Power BI / Dashboard
  WebHDFS puerto 9870           HDFS RPC puerto 9000                 HDFS RPC puerto 9000
```

El formato **Parquet** en Bronze y Silver aprovecha compresión columnar y schema nativo de Spark. **CSV** en Gold por compatibilidad directa con Power BI (solo 286 filas, el peso es irrelevante).

---

## 📋 Índice de Fases

| Fase | Descripción |
|:----:|------------|
| [0](#fase-0--infraestructura-del-cluster) | Instalación y configuración del cluster Hadoop + Spark + ZeroTier |
| [1](#fase-1--diagnóstico-y-reparación-del-cluster) | Diagnóstico y reparación del nodo caído |
| [2](#fase-2--ingesta-a-capa-bronce) | Ingesta de datos crudos a HDFS (Bronze) |
| [3](#fase-3--limpieza-a-capa-silver) | Transformaciones y limpieza con PySpark (Silver) |
| [4](#fase-4--kpis-a-capa-gold) | Cálculo de KPIs de negocio (Gold) |
| [5](#fase-5--dashboard-de-visualización) | Dashboard interactivo con Streamlit + Plotly |
| [6](#fase-6--monitoreo-y-troubleshooting) | Monitoreo, Web UIs y resolución de problemas |

---

## 📊 Resumen del Pipeline

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│   leo    │     │ XUBUNTU  │     │  DEBIAN  │     │ isait-VB │
│ 10.61.61 │     │10.61.61. │     │10.61.61. │     │10.61.61. │
│   .105   │     │   .12    │     │   .65    │     │   .7     │
├──────────┤     ├──────────┤     ├──────────┤     ├──────────┤
│NameNode  │     │DataNode  │     │DataNode  │     │DataNode  │
│Resource  │     │NodeMgmt  │     │NodeMgmt  │     │NodeMgmt  │
│Manager   │     │          │     │          │     │          │
│DataNode  │     │          │     │          │     │          │
│NodeMgmt  │     │          │     │          │     │          │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
     │                                                    │
     └────────────── ZeroTier VPN ────────────────────────┘
                            │
                    ┌───────┴───────┐
                    │   /lakehouse/  │
                    │  ┌───────────┐ │
                    │  │   🟤      │ │
                    │  │  Bronze   │ │  45 MB Parquet (crudo)
                    │  ├───────────┤ │
                    │  │   ⚪      │ │
                    │  │  Silver   │ │  2.9M registros (limpio)
                    │  ├───────────┤ │
                    │  │   🟡      │ │
                    │  │   Gold    │ │  KPIs en CSV
                    │  └───────────┘ │
                    └───────┬───────┘
                            │
                    ┌───────┴───────┐
                    │   Streamlit   │
                    │  Dashboard    │
                    │  :8501        │
                    └───────────────┘
```

### Métricas clave

| Métrica | Valor |
|---------|-------|
| Registros procesados | 3,066,766 |
| Tasa de limpieza | 94.8% (5.2% descartados) |
| Tiempo total pipeline | ~3.5 minutos |
| Workers | 3 × (4GB RAM, 2 cores) |
| Capacidad HDFS | 529 GB |
| Nodos del cluster | 4 |

---

## Fase 0 — Infraestructura del Cluster

### Stack instalado

| Componente | Versión | Método |
|-----------|:-------:|--------|
| Hadoop | 3.3.6 | Tarball en `/usr/local/hadoop` |
| Spark | 3.5.0 | `pip install pyspark` |
| Java | OpenJDK 11 | `apt` |
| ZeroTier | latest | Script oficial |
| Python | 3.13 | `apt` |

### Arquitectura de red

Los 4 nodos se comunican via **ZeroTier** (VPN privada), cada uno con IP estática:

| Nodo | IP ZeroTier | Roles |
|:----:|:-----------:|-------|
| leo | 10.61.61.105 | NameNode + ResourceManager + DataNode + NodeManager |
| XUBUNTU | 10.61.61.12 | DataNode + NodeManager |
| DEBIAN | 10.61.61.65 | DataNode + NodeManager |
| isait-VirtualBox | 10.61.61.7 | DataNode + NodeManager |

### Puertos clave

| Puerto | Servicio | Nodo |
|:------:|----------|:----:|
| 9000 | NameNode RPC | leo |
| 9870 | NameNode Web UI | leo |
| 8088 | ResourceManager Web UI | leo |
| 9864 | DataNode HTTP | todos |
| 8042 | NodeManager Web UI | todos |

📄 [Documentación completa →](docs/fase_0_instalacion_cluster.md)

---

## Fase 1 — Diagnóstico y Reparación del Cluster

### Problema

El nodo `isait-VirtualBox` no aparecía en YARN. Cluster operando al 75%.

### Causa raíz

El NodeManager se ejecutaba como usuario `isait`, pero los directorios Hadoop pertenecen a `hadoop:hadoop`. El directorio `userlogs` (`drwxr-xr-x`) no permitía escritura → YARN marcaba el nodo como `UNHEALTHY`.

### Solución

```bash
chmod 777 /opt/hadoop/logs/userlogs
sudo -u hadoop hdfs --daemon start datanode
sudo -u hadoop yarn --daemon start nodemanager
```

### Resultado

4/4 nodos RUNNING ✅ | 529 GB HDFS | 0 bloques corruptos

📄 [Documentación completa →](docs/fase_1_reparacion_cluster.md)

---

## Fase 2 — Ingesta a Capa Bronze

### Script

[`bronze_ingest.py`](bronze_ingest.py) — Python puro (sin Spark)

### Dataset

Yellow Taxi Trips NYC — Enero 2023 (~45 MB, ~3M registros)

```python
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
```

### Flujo

```
URL pública → streaming 1MB chunks → /tmp/ → WebHDFS → /lakehouse/bronze/
```

### Decisiones técnicas

| Aspecto | Elección | Alternativa fallida |
|---------|----------|-------------------|
| Librería HDFS | `hdfs` (WebHDFS, puerto 9870) | `hdfs3` (RPC, incompatible) |
| Permisos | String octal `"755"` | Entero `0o755` |
| Dataset | Yellow Taxi Parquet | Online Retail XLSX |

### Bugs corregidos

1. Puerto 9000 (RPC) → 9870 (HTTP)
2. `hdfs3` → `hdfs`
3. `permission=0o755` → `permission="755"`
4. Dataset incorrecto

📄 [Documentación completa →](docs/fase_2_ingesta_bronze.md)

---

## Fase 3 — Limpieza a Capa Silver

### Script

[`procesar_lakehouse.py`](procesar_lakehouse.py) — Función `etapa2_silver()`

### Transformaciones

| Operación | Expresión | Propósito |
|-----------|-----------|-----------|
| Filtro | `passenger_count > 0` | Eliminar sin pasajeros |
| Filtro | `trip_distance > 0` | Eliminar sin distancia |
| Casteo | `.cast("timestamp")` | Tipado correcto de fechas |
| Columna | `duracion_minutos` | Duración del viaje |
| Filtro | `duracion_minutos > 0` | Eliminar tiempos negativos |

### SparkSession para YARN + ZeroTier

```python
SparkSession.builder
    .master("yarn")
    .config("spark.driver.host", "10.61.61.105")
    .config("spark.executor.instances", "3")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.network.timeout", "800s")
```

### Resultados

- **3,066,766** registros leídos
- **2,906,607** registros limpios (5.2% descartados)
- **265** particiones por PULocationID

### Bugs corregidos

1. `deployMode()` no es método del Builder
2. `HADOOP_CONF_DIR` no definido
3. `SPARK_LOCAL_IP` para forzar IP ZeroTier
4. Permisos 777 en `/lakehouse`

📄 [Documentación completa →](docs/fase_3_limpieza_silver.md)

---

## Fase 4 — KPIs a Capa Gold

### Script

[`procesar_lakehouse.py`](procesar_lakehouse.py) — Función `etapa3_gold()`

### KPI 1 — Financiero (💰)

Ingreso total y propina promedio por hora del día.

```python
df.groupBy("hora").agg(
    F.sum("total_amount"), F.avg("tip_amount"), F.count("*")
)
```

**Salida:** 24 filas | `/lakehouse/gold/kpi_financiero/`

### KPI 2 — Operativo (⚙️)

Duración y distancia promedio por número de pasajeros.

```python
df.groupBy("passenger_count").agg(
    F.avg("duracion_minutos"), F.avg("trip_distance"), F.count("*")
)
```

**Salida:** 8 filas | `/lakehouse/gold/kpi_operativo/`

### KPI 3 — Demanda (📍)

Total de viajes por zona de recogida (ranking descendente).

```python
df.groupBy("PULocationID").agg(F.count("*")).orderBy(F.desc("count"))
```

**Salida:** 254 filas | `/lakehouse/gold/kpi_demanda/`

### Formato

Todos los KPIs se exportan como **CSV con cabecera** para compatibilidad con Power BI.

📄 [Documentación completa →](docs/fase_4_kpis_gold.md)

---

## Fase 5 — Dashboard de Visualización

### Script

[`dashboard_kpis.py`](dashboard_kpis.py) — Streamlit + Plotly + Pandas

### Lectura desde HDFS

Usa `hdfs dfs -cat` (RPC puerto 9000) en lugar de WebHDFS para evitar problemas de resolución DNS con ZeroTier.

```python
subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-cat", "/*.csv"])
```

### Visualizaciones

| KPI | Tipo de gráfico | Interactividad |
|:---:|:---------------:|:--------------:|
| Financiero | Doble eje Y (barras + línea) + barras de volumen | Métricas destacadas |
| Operativo | Doble eje Y (barras + línea) + donut | Métricas por pasajero |
| Demanda | Barras horizontales con slider Top N | Slider 10–50 zonas |

### Ejecución

```bash
streamlit run dashboard_kpis.py --server.address 10.61.61.105
```

Disponible en: `http://10.61.61.105:8501`

📄 [Documentación completa →](docs/fase_5_dashboard_kpis.md)

---

## Fase 6 — Monitoreo y Troubleshooting

### Spark History Server

```bash
bash $SPARK_HOME/sbin/start-history-server.sh \
  --properties-file /tmp/spark-defaults.conf
```

### Web UIs

| Servicio | URL |
|----------|-----|
| NameNode | http://10.61.61.105:9870 |
| ResourceManager | http://10.61.61.105:8088 |
| Spark History Server | http://10.61.61.105:18080 |

### Problemas comunes

| Problema | Solución |
|----------|----------|
| Spark usa IP WiFi | `export SPARK_LOCAL_IP=10.61.61.105` |
| HADOOP_CONF_DIR no definido | Exportar variables de entorno |
| Permission denied en HDFS | `chmod 777 /lakehouse` |
| WebHDFS falla por DNS | Usar `hdfs dfs -cat` (RPC) |
| History Server vacío | Habilitar `spark.eventLog.enabled` |

📄 [Documentación completa →](docs/fase_6_monitoreo_troubleshooting.md)

---

## 🚀 Ejecución del Pipeline

### Orquestación

El pipeline se ejecuta desde un solo punto de entrada. La función `main()` en `procesar_lakehouse.py` orquesta todo el flujo:

```
main()
 ├── 1. verificar_entorno()     → ¿PySpark instalado?
 ├── 2. crear_spark()           → SparkSession en YARN (3 executors)
 ├── 3. etapa2_silver(spark)    → Bronze → Silver (2.9M registros)
 └── 4. etapa3_gold(spark, df)  → Silver → Gold (3 KPIs en CSV)
```

### Paso a paso

```bash
# 1. Ingesta a Bronze (solo la primera vez)
python3 bronze_ingest.py

# 2. Pipeline Silver + Gold sobre YARN
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
SPARK_LOCAL_IP=10.61.61.105 \
spark-submit --master yarn --deploy-mode client procesar_lakehouse.py

# 3. Dashboard
streamlit run dashboard_kpis.py --server.address 10.61.61.105
```

### Lo que ocurre en YARN al ejecutar spark-submit

| Momento | Estado | Contenedores |
|---------|:------:|:------------:|
| `spark-submit` envía la app | `ACCEPTED` | 0 |
| ResourceManager asigna AM | `RUNNING` | 1 (ApplicationMaster) |
| AM negocia 3 executors | `RUNNING` | 4 (AM + 3 executors) |
| Transformaciones en paralelo | `RUNNING` | 4 |
| `spark.stop()` | `FINISHED` | 0 |

---

## 🗂️ Estructura del proyecto

```
/home/leo/Documentos/Big data/
├── bronze_ingest.py              # Fase 2 — Ingesta a Bronze
├── procesar_lakehouse.py         # Fase 3+4 — Silver + Gold (pipeline completo)
├── dashboard_kpis.py             # Fase 5 — Dashboard
├── README.md                     # Este archivo
└── docs/
    ├── fase_0_instalacion_cluster.md   # ZeroTier + Hadoop + Spark
    ├── fase_1_reparacion_cluster.md    # Diagnóstico nodo isait
    ├── fase_2_ingesta_bronze.md        # Bronze: ingesta de datos
    ├── fase_3_limpieza_silver.md       # Silver: limpieza con PySpark
    ├── fase_4_kpis_gold.md             # Gold: KPIs + pipeline completo
    ├── fase_5_dashboard_kpis.md        # Dashboard Streamlit
    └── fase_6_monitoreo_troubleshooting.md  # Monitoreo + errores
```

---

## 🛠️ Stack tecnológico

| Componente | Versión |
|-----------|:-------:|
| Hadoop | 3.3.6 |
| Apache Spark | 3.5.0 |
| Python | 3.13 |
| PySpark | 3.5.0 |
| Streamlit | latest |
| Plotly | latest |
| ZeroTier | latest |

---

*Proyecto implementado sobre cluster Hadoop/Spark de 4 nodos con ZeroTier.*  
*Dataset: NYC Taxi & Limousine Commission — Yellow Taxi Trips (Enero 2023)*
