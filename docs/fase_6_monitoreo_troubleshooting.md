# Fase 6 — Monitoreo y Troubleshooting

## Objetivo

Centralizar las herramientas de monitoreo, los comandos de operación diaria y la referencia de todos los problemas encontrados con sus soluciones.

---

## 1. Spark History Server

El History Server permite ver el detalle de jobs Spark aunque ya hayan terminado.

### Configuración

```bash
# Ubicación de Spark (dentro de PySpark)
export SPARK_HOME=/home/leo/.local/lib/python3.13/site-packages/pyspark

# Crear directorio para event logs
mkdir -p /tmp/spark-events

# Archivo de configuración
cat > /tmp/spark-defaults.conf << 'EOF'
spark.eventLog.enabled=true
spark.eventLog.dir=file:///tmp/spark-events
spark.history.fs.logDirectory=file:///tmp/spark-events
EOF

# Iniciar el servicio
bash $SPARK_HOME/sbin/start-history-server.sh \
  --properties-file /tmp/spark-defaults.conf
```

### Verificar

```bash
jps | grep HistoryServer
# Abrir http://localhost:18080
```

### Requisito en el código

Cada job debe tener event log habilitado en la SparkSession:

```python
.config("spark.eventLog.enabled", "true")
.config("spark.eventLog.dir", "file:///tmp/spark-events")
```

---

## 2. Web UIs del Cluster

| Servicio | URL | ¿Qué muestra? |
|----------|-----|---------------|
| **NameNode** | http://10.61.61.105:9870 | Archivos HDFS, DataNodes, capacidad |
| **NameNode Explorer** | http://10.61.61.105:9870/explorer.html | Navegador visual de HDFS |
| **ResourceManager** | http://10.61.61.105:8088 | Aplicaciones YARN, nodos, logs |
| **Spark History Server** | http://10.61.61.105:18080 | Jobs Spark pasados |
| **Spark UI** | http://10.61.61.105:4040 | Job activo (solo durante ejecución) |

### Workers via ZeroTier

| Worker | DataNode | NodeManager |
|--------|----------|-------------|
| XUBUNTU | http://10.61.61.12:9864 | http://10.61.61.12:8042 |
| DEBIAN | http://10.61.61.65:9864 | http://10.61.61.65:8042 |
| isait-VirtualBox | http://10.61.61.7:9864 | http://10.61.61.7:8042 |

---

## 3. Comandos Esenciales

### Estado del cluster

```bash
# Nodos YARN
yarn node -list

# Estado HDFS
hdfs dfsadmin -report

# Procesos Java
jps
```

### Pipeline

```bash
# Ejecutar pipeline completo (Silver + Gold)
cd "/home/leo/Documentos/Big data"
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
SPARK_LOCAL_IP=10.61.61.105 \
spark-submit --master yarn --deploy-mode client procesar_lakehouse.py

# Ingesta Bronze (solo si se necesita recargar datos)
python3 bronze_ingest.py

# Dashboard
streamlit run dashboard_kpis.py --server.address 10.61.61.105
```

### Inspeccionar datos en HDFS

```bash
# Estructura completa
hdfs dfs -ls -R /lakehouse/

# Ver contenido de KPIs
hdfs dfs -cat /lakehouse/gold/kpi_financiero/*.csv | head
hdfs dfs -cat /lakehouse/gold/kpi_operativo/*.csv | head
hdfs dfs -cat /lakehouse/gold/kpi_demanda/*.csv | head

# Ver particiones Silver
hdfs dfs -ls /lakehouse/silver/taxis_limpio/ | head
```

### Aplicaciones YARN

```bash
# Listar aplicaciones
yarn application -list

# Ver detalle de una app
yarn application -status application_XXXX

# Ver logs
yarn logs -applicationId application_XXXX
```

---

## 4. Todos los Problemas Encontrados y Soluciones

| # | Fase | Problema | Síntoma | Causa | Solución |
|:-:|:----:|----------|---------|-------|----------|
| 1 | 0 | **isait no aparece en YARN** | Solo 3/4 nodos RUNNING | NodeManager corriendo como `isait` sin permisos en `userlogs` | `chmod 777 userlogs` + reiniciar servicios como `hadoop` |
| 2 | 0 | **Spark usa IP WiFi** | Drivers fallan desde workers | `SPARK_LOCAL_IP` no definido | `export SPARK_LOCAL_IP=10.61.61.105` |
| 3 | 0 | **HADOOP_CONF_DIR no definido** | Spark no encuentra YARN | Variables de entorno faltantes | Exportar `HADOOP_CONF_DIR` y `YARN_CONF_DIR` |
| 4 | 2 | **hdfs3 no funciona** | Error importando librería | Librería incompatible con Python puro | Usar `hdfs` (WebHDFS) en vez de `hdfs3` |
| 5 | 2 | **Conexión rechazada en puerto 9000** | No se conecta a HDFS | Usar puerto RPC en vez de HTTP | Usar puerto 9870 para WebHDFS |
| 6 | 2 | **Permiso octal inválido** | `Failed to parse "493" as radix-8` | Entero Python en vez de string | Usar `"755"` en vez de `0o755` |
| 7 | 3 | **deployMode() no existe** | `AttributeError` en Builder | PySpark no tiene ese método | Pasar `--deploy-mode` por CLI |
| 8 | 3 | **Permission denied en HDFS** | Spark no puede escribir en `/lakehouse` | Directorio creado por `hadoop`, Spark corre como `leo` | `sudo -u hadoop hdfs dfs -chmod -R 777 /lakehouse` |
| 9 | 5 | **WebHDFS redirige por hostname** | `Failed to resolve debian.myguest...` | ZeroTier no tiene DNS interno | Usar `hdfs dfs -cat` (RPC) en vez de WebHDFS |
| 10 | 6 | **History Server vacío** | No se ven jobs pasados | Jobs sin event log habilitado | Agregar `.config("spark.eventLog.enabled", "true")` |

---

## 5. Dashboard

```bash
cd "/home/leo/Documentos/Big data"
streamlit run dashboard_kpis.py --server.address 10.61.61.105
```

Disponible en: http://10.61.61.105:8501

---

## 6. Diagrama de la Arquitectura Final

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
     │                                                │
     └────────────── ZeroTier VPN ────────────────────┘
                            │
                    ┌───────┴───────┐
                    │   HDFS 529GB  │
                    │  /lakehouse/  │
                    │ bronze/silver │
                    │ /gold        │
                    └───────────────┘
                            │
                    ┌───────┴───────┐
                    │   PySpark     │
                    │  sobre YARN   │
                    │ 3 executors   │
                    └───────────────┘
                            │
                    ┌───────┴───────┐
                    │   Streamlit   │
                    │  Dashboard    │
                    │ :8501         │
                    └───────────────┘
```
