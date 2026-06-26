# 📋 Bitácora de Ejecución del Pipeline Data Lakehouse

**Fecha:** 2026-06-25  
**Ingeniero:** Claude (asistente) + Leo (operador del clúster)  
**Objetivo:** Poner en marcha el pipeline completo Bronze → Silver → Gold sobre el clúster Hadoop/Spark con ZeroTier

---

## Índice

1. [Estado inicial del proyecto](#1-estado-inicial-del-proyecto)
2. [Corrección de bugs en el código PySpark](#2-corrección-de-bugs-en-el-código-pyspark)
3. [Configuración de variables de entorno Hadoop/Spark](#3-configuración-de-variables-de-entorno-hadoopspark)
4. [ETAPA 1: Ingesta a Capa Bronce](#4-etapa-1-ingesta-a-capa-bronce)
5. [Corrección de permisos HDFS](#5-corrección-de-permisos-hdfs)
6. [ETAPA 2+3: Ejecución del pipeline Silver + Gold](#6-etapa-23-ejecución-del-pipeline-silver--gold)
7. [Spark History Server](#7-spark-history-server)
8. [Resultados finales](#8-resultados-finales)
9. [Lecciones aprendidas](#9-lecciones-aprendidas)

---

## 1. Estado inicial del proyecto

### 1.1 Archivos del proyecto

```
/home/leo/Documentos/Big data/
├── bronze_ingest.py              # ETAPA 1 - Ingesta a Bronze (HDFS)
├── procesar_lakehouse.py          # ETAPA 2+3 - Silver + Gold (PySpark/YARN)
└── docs/
    ├── hadoop_cluster_info.md     # Puertos y topología del clúster
    └── documentacion_completa.md  # Documentación técnica completa
```

### 1.2 Topología del clúster (4 nodos vía ZeroTier)

| # | Hostname | IP ZeroTier | Roles |
|---|----------|-------------|-------|
| 1 | `leo` | 10.61.61.105 | NameNode + ResourceManager + DataNode + NodeManager |
| 2 | `XUBUNTU` | 10.61.61.12 | DataNode + NodeManager |
| 3 | `DEBIAN.myguest.virtualbox.org` | 10.61.61.65 | DataNode + NodeManager |
| 4 | `isait-VirtualBox` | 10.61.61.7 | DataNode + NodeManager |

---

## 2. Corrección de bugs en el código PySpark

### 2.1 Bug #1: `deployMode()` no es un método del Builder

**Síntoma:**
```
AttributeError: 'Builder' object has no attribute 'deployMode'
```

**Causa:** En PySpark, `deployMode` no es un método del `SparkSession.Builder`. Solo se pasa como argumento CLI de `spark-submit` (`--deploy-mode client`).

**Fix — Antes (línea 62):**
```python
.master("yarn")
.deployMode("client")
```

**Fix — Después:**
```python
.master("yarn")
# deployMode se pasa por CLI: spark-submit --deploy-mode client
```

### 2.2 Mejora: Event Log para Spark History Server

Se añadió configuración para que los jobs escriban logs de eventos que el History Server pueda mostrar:

```python
.config("spark.eventLog.enabled", "true")
.config("spark.eventLog.dir", "file:///tmp/spark-events")
```

---

## 3. Configuración de variables de entorno Hadoop/Spark

### 3.1 Error inicial

```bash
leo@leo:~$ spark-submit --master yarn --deploy-mode client procesar_lakehouse.py
Exception: When running with master 'yarn' either HADOOP_CONF_DIR
or YARN_CONF_DIR must be set in the environment.
```

**Causa:** `spark-submit` necesita saber dónde están los archivos de configuración de Hadoop (`core-site.xml`, `yarn-site.xml`, `hdfs-site.xml`).

### 3.2 Solución: variables de entorno + IP ZeroTier

**Variables requeridas cada vez que se ejecuta spark-submit:**

```bash
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop    # Configuración de Hadoop
YARN_CONF_DIR=/opt/hadoop/etc/hadoop      # Configuración de YARN
SPARK_LOCAL_IP=10.61.61.105              # Forzar interfaz ZeroTier
```

**Advertencia del WARN inicial:**
```
WARN Utils: Your hostname, leo resolves to a loopback address: 127.0.1.1;
using 10.70.84.39 instead (on interface wlo1)
```
Spark detectaba la IP de la WiFi (`wlo1`) en vez de ZeroTier. `SPARK_LOCAL_IP` fuerza la IP correcta.

### 3.3 Persistencia en `~/.bashrc`

Para no escribir las variables cada vez, se añadieron al final de `~/.bashrc`:

```bash
# Hadoop & Spark - ZeroTier cluster
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_LOCAL_IP=10.61.61.105
```

---

## 4. ETAPA 1: Ingesta a Capa Bronce

### 4.1 Problema inicial

**`bronze_ingest.py`** en disco todavía tenía la versión antigua:
- ❌ Dataset: `Online Retail.xlsx` desde UCI (datos equivocados)
- ❌ Librería: `hdfs3` (protocolo RPC nativo, incompatible con Python puro)
- ❌ Puerto: `9000` (RPC binario, solo para Java/Spark)

**`documentacion_completa.md`** contenía la versión corregida:
- ✅ Dataset: `yellow_tripdata_2023-01.parquet` (Yellow Taxi NYC)
- ✅ Librería: `hdfs` (WebHDFS via HTTP/REST)
- ✅ Puerto: `9870` (HTTP, accesible desde Python)

### 4.2 Actualización del script

Se reescribió `bronze_ingest.py` con:
- URL correcta: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet`
- WebHDFS via `hdfs.InsecureClient("http://leo:9870", user="hadoop")`
- Streaming de descarga en chunks de 1MB

### 4.3 Instalación de dependencias

```bash
pip install --break-system-packages hdfs
```
> Nota: Debian 13 usa entornos Python gestionados externamente (PEP 668). Se requirió `--break-system-packages`.

### 4.4 Corrección de permisos octal en WebHDFS

**Error:**
```
Invalid value for webhdfs parameter "permission": Failed to parse "493" as a radix-8 short integer.
```

**Causa:** La librería `hdfs` espera permisos como string octal (`"755"`), no como entero Python (`0o755`).

**Fix:**
```python
# Antes (falla)
client.makedirs(hdfs_dir, permission=0o755)
client.write(..., permission=0o644)

# Después (funciona)
client.makedirs(hdfs_dir, permission="755")
client.write(..., permission="644")
```

### 4.5 Ejecución exitosa

```bash
python3 bronze_ingest.py
```

```
INFO | Descargando yellow_tripdata_2023-01.parquet → /tmp/...
INFO | Descarga completa: 45 MB (47673370 bytes)
INFO | Subiendo a HDFS: → /lakehouse/bronze/yellow_tripdata_2023-01.parquet
INFO | ETAPA 1 finalizada ✅
```

### 4.6 Verificación

```bash
hdfs dfs -ls /lakehouse/bronze/
```

```
-rw-r--r--   3 hadoop supergroup   47673370  2026-06-25 20:26
/lakehouse/bronze/yellow_tripdata_2023-01.parquet

45.5 MB × réplica 3 = 136.4 MB total en HDFS
```

> ✅ ETAPA 1 completada. Dato crudo en Capa Bronce.

---

## 5. Corrección de permisos HDFS

### 5.1 Error en ETAPA 2: Permission denied

```
org.apache.hadoop.security.AccessControlException:
Permission denied: user=leo, access=WRITE, inode="/lakehouse":hadoop:supergroup:drwxr-xr-x
```

**Causa:** El directorio `/lakehouse` fue creado por el usuario `hadoop` (vía WebHDFS en `bronze_ingest.py`). Spark se ejecuta como usuario `leo`, que no es dueño ni pertenece al grupo `supergroup`. Los permisos `755` impiden que `leo` cree subdirectorios.

### 5.2 Solución

```bash
# Como usuario hadoop (dueño del directorio):
sudo -u hadoop /opt/hadoop/bin/hdfs dfs -chmod -R 777 /lakehouse

# Verificación:
hdfs dfs -ls /lakehouse/
```

```
drwxrwxrwx   /lakehouse/bronze    ← leo ya puede escribir
drwxrwxrwx   /lakehouse/silver    ← creado para la etapa 2
drwxrwxrwx   /lakehouse/gold      ← creado para la etapa 3
```

---

## 6. ETAPA 2+3: Ejecución del pipeline Silver + Gold

### 6.1 Comando de ejecución

```bash
cd "/home/leo/Documentos/Big data"
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
SPARK_LOCAL_IP=10.61.61.105 \
spark-submit --master yarn --deploy-mode client procesar_lakehouse.py
```

### 6.2 Trazado de ejecución

```
ETAPA                         | TIEMPO    | RESULTADO
══════════════════════════════╪═══════════╪══════════════════════════════════════
1. Verificación PySpark       | instant.  | ✓ PySpark 3.5.0 disponible
2. Creación SparkSession      | ~2 min    | ✓ YARN, 3 executors × 4GB/2 cores
   - Upload spark_libs.zip    |           |   Subida de JARs a HDFS staging
   - Upload pyspark.zip       |           |
   - Submit to ResourceManager|           |   application_...0005 ACCEPTED → RUNNING
3. ETAPA 2: Lectura Bronze    | ~8s       |   3,066,766 registros leídos ✅
4. ETAPA 2: Limpieza          | ~13s      |   2,906,607 registros (5.2% descarte) ✅
5. ETAPA 2: Escritura Silver  | ~2 min    |   Parquet particionado por PULocationID ✅
6. ETAPA 3: KPI Financiero    | ~29s      |   24 filas (ingreso/propina por hora) ✅
7. ETAPA 3: KPI Operativo     | ~14s      |   8 filas (duración/dist por pasajeros) ✅
8. ETAPA 3: KPI Demanda       | ~26s      |   254 filas (viajes por zona) ✅
9. SparkSession cerrada       | instant.  |   Recursos liberados en YARN ✅
──────────────────────────────┴───────────┴──────────────────────────────────────
   TIEMPO TOTAL               | ~3.5 min  |   ÉXITO COMPLETO
```

### 6.3 Logs clave de la ejecución

```
2026-06-25 20:47:31 | Registros leídos desde Bronze: 3066766
2026-06-25 20:47:44 | Registros tras limpieza: 2906607 (descartados: 160159 / 5.2%)
2026-06-25 20:49:55 | ✓ ETAPA 2 completada. Silver guardado
2026-06-25 20:50:24 |   ✓ KPI Financiero → .../gold/kpi_financiero (24 filas)
2026-06-25 20:50:38 |   ✓ KPI Operativo → .../gold/kpi_operativo (8 filas)
2026-06-25 20:51:04 |   ✓ KPI Demanda → .../gold/kpi_demanda (254 filas)
2026-06-25 20:51:04 | ✓ PIPELINE COMPLETO: Bronze → Silver → Gold
```

### 6.4 Estructura resultante en HDFS

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

---

## 7. Spark History Server

### 7.1 Configuración

Se creó `/tmp/spark-defaults.conf`:
```properties
spark.eventLog.enabled=true
spark.eventLog.dir=file:///tmp/spark-events
spark.history.fs.logDirectory=file:///tmp/spark-events
```

### 7.2 Inicio del servicio

```bash
export SPARK_HOME=/home/leo/.local/lib/python3.13/site-packages/pyspark
mkdir -p /tmp/spark-events
bash $SPARK_HOME/sbin/start-history-server.sh --properties-file /tmp/spark-defaults.conf
```

### 7.3 URLs de monitoreo Spark

| Interfaz | URL | Disponibilidad |
|----------|-----|----------------|
| **Spark UI** (job en vivo) | `http://localhost:4040` | Solo durante ejecución |
| **Spark History Server** | `http://localhost:18080` | Jobs pasados y presentes (con event log) |
| **YARN ResourceManager** | `http://localhost:8088` | Todas las apps (Spark y no-Spark) |

### 7.4 Web UIs del clúster Hadoop

| Servicio | URL | ¿Qué muestra? |
|----------|-----|---------------|
| NameNode | `http://localhost:9870` | HDFS: archivos, DataNodes, capacidad, bloques |
| NameNode Explorer | `http://localhost:9870/explorer.html` | Navegador visual de archivos HDFS |
| NameNode DataNodes | `http://localhost:9870/dfshealth.html#tab-datanode` | DataNodes vivos/muertos |
| SecondaryNameNode | `http://localhost:9868` | Checkpoints del NameNode |
| ResourceManager | `http://localhost:8088` | Apps YARN, nodos, colas, schedulers |
| DataNode (leo) | `http://localhost:9864` | Estado del DataNode local |
| NodeManager (leo) | `http://localhost:8042` | Contenedores YARN locales |

**Workers (desde leo, usando IPs ZeroTier):**

| Worker | DataNode | NodeManager |
|--------|----------|-------------|
| XUBUNTU | `http://10.61.61.12:9864` | `http://10.61.61.12:8042` |
| DEBIAN | `http://10.61.61.65:9864` | `http://10.61.61.65:8042` |
| isait-VirtualBox | `http://10.61.61.7:9864` | `http://10.61.61.7:8042` |

---

## 8. Resultados finales

### 8.1 KPIs generados (Capa Oro)

| KPI | Descripción | Filas | Columnas |
|-----|-------------|:-----:|----------|
| 💰 **kpi_financiero** | Ingreso total y propina promedio por hora del día | 24 | `hora`, `ingreso_total`, `propina_promedio`, `total_viajes` |
| ⚙️ **kpi_operativo** | Duración y distancia promedio por nº de pasajeros | 8 | `passenger_count`, `duracion_promedio_min`, `distancia_promedio_km`, `total_viajes` |
| 📍 **kpi_demanda** | Total viajes por zona de recogida (descendente) | 254 | `PULocationID`, `total_viajes` |

### 8.2 Métricas del pipeline

| Indicador | Valor |
|-----------|-------|
| Registros crudos (Bronze) | 3,066,766 |
| Registros limpios (Silver) | 2,906,607 |
| Registros descartados | 160,159 (5.2%) |
| Tamaño Bronze | 45.5 MB (×3 réplicas = 136.4 MB) |
| Tiempo total del pipeline | ~3.5 minutos |
| Workers utilizados | 3 × (4 GB RAM, 2 cores) |
| Shuffle partitions | 12 |

### 8.3 Estado del clúster al finalizar

| Indicador | Estado |
|-----------|--------|
| Nodos YARN | 4/4 RUNNING ✅ |
| DataNodes HDFS | 4/4 activos ✅ |
| Capacidad HDFS | 529 GB total, 149 GB disponible |
| Bloques corruptos | 0 ✅ |
| Bloques under-replicated | 0 ✅ |

---

## 9. Lecciones aprendidas

### 9.1 Problemas encontrados y sus soluciones

| # | Problema | Causa raíz | Solución |
|---|----------|-----------|----------|
| 1 | `deployMode()` no existe | PySpark Builder no tiene ese método | Se pasa por CLI (`--deploy-mode client`) |
| 2 | `HADOOP_CONF_DIR` no definido | Spark no encuentra configs YARN | Exportar variable con ruta a `/opt/hadoop/etc/hadoop` |
| 3 | Spark usa IP WiFi en vez de ZeroTier | Hostname `leo` → loopback → WiFi | `export SPARK_LOCAL_IP=10.61.61.105` |
| 4 | Archivo no existe en HDFS | `bronze_ingest.py` no se había ejecutado | Actualizar script + instalar `hdfs` + ejecutar |
| 5 | Permission denied al escribir Silver/Gold | `/lakehouse` creado por `hadoop`, Spark corre como `leo` | `hdfs dfs -chmod 777 /lakehouse` como usuario `hadoop` |
| 6 | Error `permission` octal en WebHDFS | La librería `hdfs` espera strings (`"755"`), no enteros (`0o755`) | Usar strings para permisos |
| 7 | History Server vacío | Jobs anteriores sin `spark.eventLog.enabled` | Añadir config al código Spark |

### 9.2 Comandos esenciales para el día a día

```bash
# Ver nodos YARN
yarn node -list

# Ver estado HDFS
hdfs dfsadmin -report

# Ver estructura de archivos
hdfs dfs -ls -R /lakehouse/

# Ver contenido de KPIs
hdfs dfs -cat /lakehouse/gold/kpi_financiero/*.csv | head

# Ejecutar el pipeline completo
cd "/home/leo/Documentos/Big data"
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
SPARK_LOCAL_IP=10.61.61.105 \
spark-submit --master yarn --deploy-mode client procesar_lakehouse.py

# Ver app en Spark History Server
# → Abrir http://localhost:18080 en el navegador

# Ver app en YARN
# → Abrir http://localhost:8088 en el navegador
```

### 9.3 Requisitos para futuras ejecuciones

1. ✅ Los 4 nodos deben estar con `yarn node -list` mostrando RUNNING
2. ✅ `HDFS` con DataNodes vivos (`hdfs dfsadmin -report`)
3. ✅ Variables de entorno cargadas (`source ~/.bashrc`)
4. ✅ Permisos 777 en `/lakehouse/` (si se recrea)
5. ✅ History Server corriendo si se quiere ver el historial Spark

---

*Documento generado el 25 de Junio de 2026 durante la sesión de puesta en marcha del pipeline.*
*Clúster: Hadoop 3.3.6 | Spark 3.5.0 | ZeroTier | 4 nodos (1 Driver + 3 Workers)*
