#!/usr/bin/env python3
"""
🏛️ Dashboard de KPIs — NYC Yellow Taxi 2023-01
═══════════════════════════════════════════════════════════════

Lee los 3 CSV agregados desde la capa Gold en HDFS y pinta
gráficos interactivos con Plotly para análisis de negocio.

Ejecución:
    streamlit run dashboard_kpis.py --server.address 10.61.61.105

Arquitectura:
    HDFS (RPC :9000) → hdfs dfs -cat → Pandas → Plotly → Streamlit
    Sin dependencia de WebHDFS ni redirects a DataNodes por hostname.
"""

# ── Librerías estándar ──
import subprocess             # Ejecutar comandos del sistema (hdfs dfs -cat)
from io import StringIO        # Leer string como archivo para pandas.read_csv()

# ── Librerías de visualización ──
import streamlit as st         # Framework de dashboard web interactivo
import pandas as pd            # DataFrames para manipular los CSVs de los KPIs
import plotly.express as px    # Gráficos express de Plotly (barras, pie)
import plotly.graph_objects as go  # Gráficos avanzados de Plotly (doble eje Y)

# ═══════════════════════════════════════════════════════════════
# CONFIGURACIÓN DEL ENTORNO
# ═══════════════════════════════════════════════════════════════

# Ruta base en HDFS donde Spark escribió los 3 KPIs en formato CSV
GOLD_BASE = "/lakehouse/gold"

# Cliente nativo de Hadoop — usa RPC binario en puerto 9000,
# NO genera redirects HTTP a DataNodes (a diferencia de WebHDFS en :9870).
# Esto evita el error "Failed to resolve debian.myguest.virtualbox.org"
HDFS_CMD = "/opt/hadoop/bin/hdfs"

# Mapeo de cada KPI a su carpeta en HDFS
# Cada carpeta contiene archivos part-XXXXX.csv generados por Spark
KPI_PATHS = {
    "financiero": f"{GOLD_BASE}/kpi_financiero",   # 24 filas: ingreso/propina por hora
    "operativo":  f"{GOLD_BASE}/kpi_operativo",    # 8 filas: métricas por passenger_count
    "demanda":    f"{GOLD_BASE}/kpi_demanda",       # 254 filas: viajes por PULocationID
}

# ── Configuración de la página en Streamlit ──
st.set_page_config(
    page_title="NYC Taxi KPIs — Data Lakehouse",   # Título en la pestaña del navegador
    page_icon="🚕",                                 # Emoji de taxi como favicon
    layout="wide",                                  # Usar todo el ancho de pantalla
)


# ═══════════════════════════════════════════════════════════════
# FUNCIÓN DE LECTURA DESDE HDFS (CAPA GOLD)
# ═══════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner="Leyendo KPIs desde HDFS…")
def leer_csv_hdfs(hdfs_path: str) -> pd.DataFrame:
    """
    Lee todos los archivos CSV dentro de una carpeta en HDFS y los
    devuelve como un DataFrame de Pandas unificado.

    Mecanismo:
        1. Ejecuta 'hdfs dfs -cat /ruta/*.csv' vía subprocess.
        2. Concatena el output de todos los archivos en un solo string.
        3. Filtra líneas de header repetidas (hdfs -cat no las elimina).
        4. Parsea el CSV limpio con pandas.read_csv().

    ¿Por qué subprocess + hdfs dfs -cat y NO WebHDFS (InsecureClient)?
    ─────────────────────────────────────────────────────────────────
    WebHDFS (puerto 9870) redirige las lecturas al DataNode físico
    usando su HOSTNAME (ej. 'debian.myguest.virtualbox.org').
    Como nuestra red ZeroTier no tiene DNS interno, esa resolución
    falla con NameResolutionError.

    El cliente nativo 'hdfs dfs -cat' usa el protocolo RPC (puerto 9000)
    y el NameNode maneja la transferencia sin redirects HTTP.

    Parámetros
    ----------
    hdfs_path : str
        Ruta absoluta en HDFS a la carpeta del KPI.
        Ej: '/lakehouse/gold/kpi_financiero'

    Retorna
    -------
    pd.DataFrame
        DataFrame con los datos del KPI, o DataFrame vacío si hay error.
    """
    try:
        # ── Ejecutar hdfs dfs -cat con wildcard *.csv ──
        #   - capture_output=True  → captura stdout y stderr
        #   - text=True            → devuelve string (no bytes)
        #   - timeout=30           → máximo 30 segundos de espera
        result = subprocess.run(
            [HDFS_CMD, "dfs", "-cat", f"{hdfs_path}/*.csv"],
            capture_output=True, text=True, timeout=30,
        )

        # Si el comando falló (código de retorno ≠ 0), mostramos el error
        if result.returncode != 0:
            st.error(f"Error leyendo HDFS: {result.stderr}")
            return pd.DataFrame()

        # ── Parsear el CSV concatenado ──
        # hdfs -cat sobre múltiples archivos concatena todo, incluyendo
        # headers repetidos. Ejemplo de salida:
        #   hora,ingreso_total,propina_promedio,total_viajes  ← header archivo 1
        #   0,2276733.2,3.51,80415
        #   ...
        #   hora,ingreso_total,propina_promedio,total_viajes  ← header archivo 2 (repetido)
        #   0,2276733.2,3.51,80415
        #   ...
        lines = result.stdout.strip().split("\n")
        if not lines:
            return pd.DataFrame()

        # Conservamos solo la primera ocurrencia del header
        header = lines[0]                         # Primera línea = nombres de columnas
        data_lines = [line for line in lines[1:]   # Resto de líneas...
                      if line != header]           # ...excepto headers repetidos

        # Reconstruimos CSV limpio: header único + todas las filas de datos
        csv_clean = "\n".join([header] + data_lines)
        return pd.read_csv(StringIO(csv_clean))    # StringIO → pandas lee como archivo

    except Exception as e:
        # Cualquier excepción (timeout, archivo no encontrado, etc.)
        st.error(f"Error leyendo HDFS: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
# KPI 1 — FINANCIERO: Ingreso y propina por hora del día
# ═══════════════════════════════════════════════════════════════

def graficar_kpi_financiero(df: pd.DataFrame):
    """
    Genera 2 gráficos + 3 métricas destacadas para el KPI Financiero.

    Columnas esperadas en df:
        hora (int)              → 0..23, hora de recogida
        ingreso_total (float)   → Suma de total_amount para esa hora
        propina_promedio (float)→ Promedio de tip_amount para esa hora
        total_viajes (int)      → Cantidad de viajes en esa hora

    Gráficos:
        1. Barras (ingreso) + Línea (propina) con doble eje Y
        2. Barras de volumen de viajes con escala de color
    """
    st.subheader("💰 KPI Financiero — Ingreso y propina por hora del día")

    # ── Métricas destacadas (cards arriba) ──
    col1, col2, col3 = st.columns(3)                                   # 3 columnas iguales

    # Encontramos la hora con mayor ingreso, mayor volumen y mejor propina
    hora_pico_ingreso = df.loc[df["ingreso_total"].idxmax()]           # Fila con máximo ingreso
    hora_pico_viajes  = df.loc[df["total_viajes"].idxmax()]            # Fila con máximo volumen
    hora_mejor_propina = df.loc[df["propina_promedio"].idxmax()]      # Fila con máxima propina

    # Cada métrica muestra: valor principal + delta (la hora correspondiente)
    col1.metric("💵 Ingreso total máximo",
                f"${hora_pico_ingreso['ingreso_total']:,.0f}",        # Ej: $5,620,315
                f"{int(hora_pico_ingreso['hora'])}h")                 # Ej: 17h
    col2.metric("🚕 Máx. volumen de viajes",
                f"{int(hora_pico_viajes['total_viajes']):,}",          # Ej: 205,034
                f"{int(hora_pico_viajes['hora'])}h")
    col3.metric("🪙 Mejor propina promedio",
                f"${hora_mejor_propina['propina_promedio']:.2f}",     # Ej: $3.83
                f"{int(hora_mejor_propina['hora'])}h")

    # ── Gráfico 1: Doble eje Y (barras = ingreso, línea = propina) ──
    fig = go.Figure()                                                  # Figura vacía de Plotly

    # Barra: ingreso total por hora (eje Y izquierdo)
    fig.add_trace(go.Bar(
        x=df["hora"], y=df["ingreso_total"],
        name="Ingreso Total (USD)",                                    # Etiqueta en la leyenda
        marker_color="#636EFA",                                        # Azul corporativo
        yaxis="y1",                                                    # Asignar al eje Y izquierdo
    ))

    # Línea: propina promedio por hora (eje Y derecho)
    fig.add_trace(go.Scatter(
        x=df["hora"], y=df["propina_promedio"],
        name="Propina Promedio (USD)",
        marker_color="#EF553B",                                        # Rojo contraste
        mode="lines+markers",                                          # Línea con puntos
        yaxis="y2",                                                    # Asignar al eje Y derecho
    ))

    # Configuración de layout: títulos, ejes, leyenda
    fig.update_layout(
        title="Ingreso total y propina promedio por hora del día",
        xaxis=dict(title="Hora del día", tickmode="linear", dtick=1), # Eje X: 0..23, un tick por hora
        yaxis=dict(title="Ingreso Total (USD)", side="left"),          # Eje Y izquierdo
        yaxis2=dict(title="Propina Promedio (USD)", side="right",      # Eje Y derecho
                     overlaying="y", rangemode="tozero"),              # Superpuesto al izquierdo
        legend=dict(orientation="h", yanchor="bottom", y=1.02),       # Leyenda horizontal arriba
        height=450,                                                    # Altura del gráfico en px
    )
    st.plotly_chart(fig, width="stretch")                              # Renderiza el gráfico

    # ── Gráfico 2: Volumen de viajes por hora ──
    fig2 = px.bar(df,
                  x="hora", y="total_viajes",                          # Ejes
                  title="Volumen de viajes por hora",
                  labels={"hora": "Hora", "total_viajes": "Viajes"},   # Renombrar etiquetas
                  color="total_viajes",                                # Color según valor
                  color_continuous_scale="Blues")                      # Escala de azules
    fig2.update_layout(height=350, coloraxis_showscale=False)          # Sin barra de color lateral
    st.plotly_chart(fig2, width="stretch")


# ═══════════════════════════════════════════════════════════════
# KPI 2 — OPERATIVO: Duración y distancia por nº de pasajeros
# ═══════════════════════════════════════════════════════════════

def graficar_kpi_operativo(df: pd.DataFrame):
    """
    Genera 2 gráficos + 2 métricas destacadas para el KPI Operativo.

    Columnas esperadas en df:
        passenger_count (float)      → 1..8, nº de pasajeros
        duracion_promedio_min (float)→ Duración promedio en minutos
        distancia_promedio_km (float)→ Distancia promedio en km
        total_viajes (int)           → Cantidad de viajes

    Gráficos:
        1. Barras (duración) + Línea (distancia) con doble eje Y
        2. Donut chart con distribución de viajes por nº de pasajeros
    """
    st.subheader("⚙️ KPI Operativo — Rendimiento por número de pasajeros")

    # ── Métricas destacadas ──
    col1, col2 = st.columns(2)

    max_duracion = df.loc[df["duracion_promedio_min"].idxmax()]       # Fila con mayor duración
    max_distancia = df.loc[df["distancia_promedio_km"].idxmax()]      # Fila con mayor distancia

    col1.metric("⏱️ Mayor duración promedio",
                f"{max_duracion['duracion_promedio_min']:.1f} min",
                f"{int(max_duracion['passenger_count'])} pasajeros")
    col2.metric("📏 Mayor distancia promedio",
                f"{max_distancia['distancia_promedio_km']:.2f} km",
                f"{int(max_distancia['passenger_count'])} pasajeros")

    # ── Gráfico 1: Doble eje Y ──
    fig = go.Figure()

    # Barra: duración promedio (eje Y izquierdo)
    fig.add_trace(go.Bar(
        x=df["passenger_count"].astype(int),                           # Convertir 1.0 → 1 para eje limpio
        y=df["duracion_promedio_min"],
        name="Duración (min)",
        marker_color="#00CC96",                                        # Verde
    ))

    # Línea: distancia promedio (eje Y derecho)
    fig.add_trace(go.Scatter(
        x=df["passenger_count"].astype(int),
        y=df["distancia_promedio_km"],
        name="Distancia (km)",
        marker_color="#AB63FA",                                        # Púrpura
        mode="lines+markers",
        yaxis="y2",
    ))

    fig.update_layout(
        title="Duración y distancia promedio por cantidad de pasajeros",
        xaxis=dict(title="Número de pasajeros", tickmode="linear", dtick=1),
        yaxis=dict(title="Duración (min)", side="left"),
        yaxis2=dict(title="Distancia (km)", side="right", overlaying="y", rangemode="tozero"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        height=400,
    )
    st.plotly_chart(fig, width="stretch")

    # ── Gráfico 2: Donut de distribución de viajes ──
    fig2 = px.pie(df,
                  values="total_viajes",                                # Tamaño de cada porción
                  names=df["passenger_count"].astype(int),              # Etiqueta de cada porción
                  title="Distribución de viajes por nº de pasajeros",
                  hole=0.4)                                            # Agujero central → donut (0 = pie normal)
    fig2.update_traces(textinfo="percent+label")                        # Mostrar % y etiqueta
    fig2.update_layout(height=380)
    st.plotly_chart(fig2, width="stretch")


# ═══════════════════════════════════════════════════════════════
# KPI 3 — DEMANDA: Top zonas de recogida
# ═══════════════════════════════════════════════════════════════

def graficar_kpi_demanda(df: pd.DataFrame):
    """
    Genera 1 gráfico interactivo + 3 métricas para el KPI Demanda.

    Columnas esperadas en df (ordenado desc por total_viajes):
        PULocationID (int)  → ID de la zona de recogida (1..265)
        total_viajes (int)  → Cantidad de viajes desde esa zona

    Gráficos:
        1. Barras horizontales del Top N zonas (N ajustable con slider)
    """
    st.subheader("📍 KPI Demanda — Viajes por zona de recogida (PULocationID)")

    # ── Slider para elegir cuántas zonas mostrar ──
    top_n = st.slider("Mostrar top N zonas",
                      min_value=10, max_value=50,                      # Rango: 10 a 50
                      value=20,                                        # Valor por defecto: 20
                      step=5)                                          # Incrementos de 5 en 5

    # ── Métricas agregadas ──
    top = df.head(top_n)                                                # Primeras N filas (ya ordenadas desc)
    total_general = df["total_viajes"].sum()                            # Todos los viajes
    top_total = top["total_viajes"].sum()                               # Viajes del Top N

    col1, col2, col3 = st.columns(3)
    col1.metric("🗺️ Zonas con demanda registrada", f"{len(df)}")       # 254 zonas
    col2.metric("🚕 Total general de viajes", f"{int(total_general):,}")# ~2.9M viajes
    col3.metric(f"📊 Top {top_n} concentra",
                f"{top_total / total_general * 100:.1f}%")             # % de concentración

    # ── Gráfico: Barras horizontales ──
    fig = px.bar(
        top,
        x="total_viajes",                                              # Eje X = cantidad
        y=top["PULocationID"].astype(str),                             # Eje Y = ID de zona (string para categórico)
        orientation="h",                                               # Barras horizontales
        title=f"Top {top_n} zonas de recogida con mayor demanda",
        labels={"total_viajes": "Total Viajes",
                "PULocationID": "Zona (PULocationID)"},
        color="total_viajes",                                          # Intensidad según cantidad
        color_continuous_scale="OrRd",                                 # Escala naranja→rojo
        text_auto=True,                                                # Mostrar valor al final de cada barra
    )
    fig.update_layout(height=500,
                      coloraxis_showscale=False,                       # Sin barra de color lateral
                      yaxis=dict(autorange="reversed"))                # Mayor arriba, menor abajo
    st.plotly_chart(fig, width="stretch")


# ═══════════════════════════════════════════════════════════════
# MAIN — Orquestador del dashboard
# ═══════════════════════════════════════════════════════════════

def main():
    """
    Punto de entrada del dashboard.

    Flujo:
        1. Renderiza sidebar con info de infraestructura y pipeline.
        2. Lee los 3 KPIs desde HDFS.
        3. Si hay datos, los muestra en 3 tabs con gráficos y tablas.
    """

    # ──────────────────────────────────────────────────────────
    # SIDEBAR (barra lateral izquierda)
    # ──────────────────────────────────────────────────────────
    with st.sidebar:
        # Logo de NYC Taxi & Limousine Commission
        st.image("https://www.nyc.gov/assets/tlc/images/tlc_logo.png",
                 width=180)
        st.title("🚕 NYC Taxi KPIs")
        st.markdown("**Data Lakehouse** — Medallion Architecture")
        st.divider()

        # Tabla de arquitectura Medallion
        st.markdown("### 🏗️ Pipeline")
        st.markdown("""
        | Capa | Formato |
        |------|---------|
        | 🟤 Bronze | Parquet |
        | ⚪ Silver | Parquet |
        | 🟡 Gold | CSV |
        """)

        st.divider()

        # Info de infraestructura del clúster
        st.markdown("### 🖥️ Infraestructura")
        st.markdown("""
        - **Hadoop** 3.3.6
        - **Spark** 3.5.0
        - **4 nodos** (ZeroTier)
        - **HDFS** 529 GB
        """)

        st.divider()

        # Notas al pie
        st.caption("Datos: NYC TLC Yellow Taxi — Enero 2023")
        st.caption(f"Origen: `{GOLD_BASE}` vía `hdfs dfs -cat` (RPC)")

    # ──────────────────────────────────────────────────────────
    # CONTENIDO PRINCIPAL
    # ──────────────────────────────────────────────────────────

    # Título y descripción
    st.title("🚕 Dashboard de KPIs — NYC Yellow Taxi Trip Data")
    st.markdown(
        "**Pipeline Medallion:** Bronze → Silver → Gold | "
        "Motor: PySpark sobre YARN | "
        "3,066,766 registros procesados"
    )
    st.divider()

    # ── Lectura de KPIs desde HDFS ──
    # Cada llamada ejecuta hdfs dfs -cat y devuelve un DataFrame
    df_financiero = leer_csv_hdfs(KPI_PATHS["financiero"])
    df_operativo  = leer_csv_hdfs(KPI_PATHS["operativo"])
    df_demanda    = leer_csv_hdfs(KPI_PATHS["demanda"])

    # ── Validación: si algún KPI viene vacío, mostramos error y paramos ──
    if df_financiero.empty or df_operativo.empty or df_demanda.empty:
        st.error(
            "❌ No se pudieron leer los KPIs desde HDFS. "
            "Verifica que los datos existan en `/lakehouse/gold/`."
        )
        st.stop()  # Detiene la ejecución del script

    # ── Tabs para navegar entre KPIs ──
    tab1, tab2, tab3 = st.tabs(["💰 Financiero", "⚙️ Operativo", "📍 Demanda"])

    # Tab 1: KPI Financiero
    with tab1:
        graficar_kpi_financiero(df_financiero)

        # Expander colapsable con los datos en crudo
        with st.expander("📄 Ver datos brutos — KPI Financiero"):
            st.dataframe(df_financiero, width="stretch", hide_index=True)

    # Tab 2: KPI Operativo
    with tab2:
        graficar_kpi_operativo(df_operativo)

        with st.expander("📄 Ver datos brutos — KPI Operativo"):
            st.dataframe(df_operativo, width="stretch", hide_index=True)

    # Tab 3: KPI Demanda
    with tab3:
        graficar_kpi_demanda(df_demanda)

        with st.expander("📄 Ver datos brutos — KPI Demanda"):
            st.dataframe(df_demanda, width="stretch", hide_index=True)


# ═══════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()  # Ejecuta el dashboard cuando se llama con streamlit run
