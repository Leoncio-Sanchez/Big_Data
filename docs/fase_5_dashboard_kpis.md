# Fase 5 — Dashboard de KPIs con Streamlit

## Objetivo

Construir un dashboard web interactivo que lee los 3 KPIs desde la capa Gold en HDFS y los visualiza con gráficos Plotly para análisis de negocio.

**Script:** `/home/leo/Documentos/Big data/dashboard_kpis.py`

---

## 1. Stack Tecnológico

| Componente | Versión | Propósito |
|-----------|:-------:|-----------|
| **Streamlit** | latest | Framework de dashboard web |
| **Plotly Express** | latest | Gráficos rápidos (barras, donut) |
| **Plotly Graph Objects** | latest | Gráficos avanzados (doble eje Y) |
| **Pandas** | latest | DataFrames para datos tabulares |
| **subprocess** | estándar | Ejecutar `hdfs dfs -cat` en el sistema |

---

## 2. Lectura de Datos desde HDFS

### Problema con WebHDFS

WebHDFS (puerto 9870) redirige las lecturas al DataNode físico usando su **hostname** (ej. `debian.myguest.virtualbox.org`). Como ZeroTier no tiene DNS interno para esos nombres, falla con `NameResolutionError`.

### Solución: RPC nativo via CLI

```python
HDFS_CMD = "/opt/hadoop/bin/hdfs"

result = subprocess.run(
    [HDFS_CMD, "dfs", "-cat", f"{hdfs_path}/*.csv"],
    capture_output=True, text=True, timeout=30,
)
```

**Ventaja:** El protocolo RPC (puerto 9000) no redirige a DataNodes por hostname. El NameNode maneja la transferencia internamente.

### Procesamiento del CSV

Los archivos `part-*.csv` generados por Spark se concatenan con `hdfs dfs -cat`. Como cada archivo incluye su propio header, hay que limpiar los headers repetidos:

```python
lines = result.stdout.strip().split("\n")
header = lines[0]
data_lines = [line for line in lines[1:] if line != header]
csv_clean = "\n".join([header] + data_lines)
df = pd.read_csv(StringIO(csv_clean))
```

### Cache

```python
@st.cache_data(ttl=300, show_spinner="Leyendo KPIs desde HDFS…")
```

Los datos se refrescan cada 5 minutos.

---

## 3. Visualizaciones

### KPI 1 — Financiero (💰)

**Gráfico 1:** Doble eje Y — Barras (ingreso total) + Línea (propina promedio)

```python
fig.add_trace(go.Bar(x=df["hora"], y=df["ingreso_total"], yaxis="y1"))
fig.add_trace(go.Scatter(x=df["hora"], y=df["propina_promedio"], yaxis="y2"))
```

**Gráfico 2:** Barras de volumen de viajes por hora con escala de color

```python
px.bar(df, x="hora", y="total_viajes", color="total_viajes", color_continuous_scale="Blues")
```

**Métricas destacadas:**
- Hora con mayor ingreso total
- Hora con mayor volumen de viajes
- Hora con mejor propina promedio

### KPI 2 — Operativo (⚙️)

**Gráfico 1:** Doble eje Y — Barras (duración promedio) + Línea (distancia promedio)

```python
fig.add_trace(go.Bar(x=df["passenger_count"], y=df["duracion_promedio_min"]))
fig.add_trace(go.Scatter(x=df["passenger_count"], y=df["distancia_promedio_km"], yaxis="y2"))
```

**Gráfico 2:** Donut chart de distribución de viajes

```python
px.pie(df, values="total_viajes", names=df["passenger_count"].astype(int), hole=0.4)
```

**Métricas:** Mayor duración y distancia promedio por pasajero

### KPI 3 — Demanda (📍)

**Gráfico:** Barras horizontales del Top N zonas

```python
px.bar(top, x="total_viajes", y=top["PULocationID"].astype(str),
       orientation="h", color="total_viajes", color_continuous_scale="OrRd")
```

**Interactividad:** Slider para ajustar el Top N (10 a 50)

```python
top_n = st.slider("Mostrar top N zonas", min_value=10, max_value=50, value=20, step=5)
```

**Métricas:** Zonas con demanda, total de viajes, % de concentración del Top N

---

## 4. Sidebar del Dashboard

```
┌────────────────────────────────┐
│  🚕 NYC Taxi KPIs              │
│  Data Lakehouse — Medallion     │
│                                │
│  🏗️ Pipeline                   │
│  🟤 Bronze → Parquet          │
│  ⚪ Silver → Parquet          │
│  🟡 Gold → CSV                │
│                                │
│  🖥️ Infraestructura            │
│  Hadoop 3.3.6                 │
│  Spark 3.5.0                  │
│  4 nodos (ZeroTier)            │
│  HDFS 529 GB                  │
└────────────────────────────────┘
```

---

## 5. Dependencias

```bash
pip install --break-system-packages streamlit plotly pandas
```

---

## 6. Ejecución

```bash
cd "/home/leo/Documentos/Big data"
streamlit run dashboard_kpis.py --server.address 10.61.61.105
```

Dashboard disponible en: `http://10.61.61.105:8501`

---

## 7. Arquitectura Completa

```
HDFS (RPC :9000)
    │
    ▼
hdfs dfs -cat (subprocess)
    │
    ▼
StringIO → Pandas DataFrame
    │
    ▼
Plotly (gráficos interactivos)
    │
    ▼
Streamlit (3 tabs en el navegador)
    │
    ▼
http://10.61.61.105:8501
```
