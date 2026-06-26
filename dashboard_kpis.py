#!/usr/bin/env python3
"""
🏛️ Dashboard de KPIs — NYC Yellow Taxi 2023-01
Lee los CSV agregados desde la capa Gold en HDFS y pinta gráficos interactivos con Plotly.
Ejecución: streamlit run dashboard_kpis.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from hdfs import InsecureClient
from io import StringIO
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════

HDFS_URI = "http://leo:9870"              # WebHDFS (HTTP)
HDFS_USER = "hadoop"
GOLD_BASE = "/lakehouse/gold"

KPI_PATHS = {
    "financiero": f"{GOLD_BASE}/kpi_financiero",
    "operativo":  f"{GOLD_BASE}/kpi_operativo",
    "demanda":    f"{GOLD_BASE}/kpi_demanda",
}

st.set_page_config(
    page_title="NYC Taxi KPIs — Data Lakehouse",
    page_icon="🚕",
    layout="wide",
)


import subprocess
import tempfile

# ═══════════════════════════════════════════════════════════════
# LECTURA DESDE HDFS
# ═══════════════════════════════════════════════════════════════

HDFS_CMD = "/opt/hadoop/bin/hdfs"  # cliente nativo HDFS (RPC :9000, sin redirects a DataNodes)


@st.cache_data(ttl=300, show_spinner="Leyendo KPIs desde HDFS…")
def leer_csv_hdfs(hdfs_path: str) -> pd.DataFrame:
    """
    Concatena todos los archivos CSV de una carpeta en HDFS en un DataFrame.
    Usa el cliente nativo 'hdfs dfs -cat' (RPC puerto 9000), que no sufre
    los redirects a DataNodes por hostname que tiene WebHDFS.
    """
    try:
        result = subprocess.run(
            [HDFS_CMD, "dfs", "-cat", f"{hdfs_path}/*.csv"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            st.error(f"Error leyendo HDFS: {result.stderr}")
            return pd.DataFrame()

        # El output concatena todos los CSVs con sus headers repetidos;
        # filtramos para conservar solo la primera línea de header
        lines = result.stdout.strip().split("\n")
        if not lines:
            return pd.DataFrame()

        header = lines[0]
        data_lines = [line for line in lines[1:] if line != header]

        csv_clean = "\n".join([header] + data_lines)
        return pd.read_csv(StringIO(csv_clean))

    except Exception as e:
        st.error(f"Error leyendo HDFS: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
# GRÁFICOS
# ═══════════════════════════════════════════════════════════════

def graficar_kpi_financiero(df: pd.DataFrame):
    """Ingreso total, propina promedio y total viajes por hora del día."""
    st.subheader("💰 KPI Financiero — Ingreso y propina por hora del día")

    col1, col2, col3 = st.columns(3)
    hora_pico_ingreso = df.loc[df["ingreso_total"].idxmax()]
    hora_pico_viajes  = df.loc[df["total_viajes"].idxmax()]
    hora_mejor_propina = df.loc[df["propina_promedio"].idxmax()]

    col1.metric("💵 Ingreso total máximo", f"${hora_pico_ingreso['ingreso_total']:,.0f}",
                f"{int(hora_pico_ingreso['hora'])}h")
    col2.metric("🚕 Máx. volumen de viajes", f"{int(hora_pico_viajes['total_viajes']):,}",
                f"{int(hora_pico_viajes['hora'])}h")
    col3.metric("🪙 Mejor propina promedio", f"${hora_mejor_propina['propina_promedio']:.2f}",
                f"{int(hora_mejor_propina['hora'])}h")

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df["hora"], y=df["ingreso_total"],
        name="Ingreso Total (USD)",
        marker_color="#636EFA",
        yaxis="y1",
    ))
    fig.add_trace(go.Scatter(
        x=df["hora"], y=df["propina_promedio"],
        name="Propina Promedio (USD)",
        marker_color="#EF553B",
        mode="lines+markers",
        yaxis="y2",
    ))

    fig.update_layout(
        title="Ingreso total y propina promedio por hora del día",
        xaxis=dict(title="Hora del día", tickmode="linear", dtick=1),
        yaxis=dict(title="Ingreso Total (USD)", side="left"),
        yaxis2=dict(title="Propina Promedio (USD)", side="right", overlaying="y",
                     rangemode="tozero"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        height=450,
    )
    st.plotly_chart(fig, width="stretch")

    # Segundo gráfico: volumen de viajes
    fig2 = px.bar(df, x="hora", y="total_viajes",
                  title="Volumen de viajes por hora",
                  labels={"hora": "Hora", "total_viajes": "Viajes"},
                  color="total_viajes",
                  color_continuous_scale="Blues")
    fig2.update_layout(height=350, coloraxis_showscale=False)
    st.plotly_chart(fig2, width="stretch")


def graficar_kpi_operativo(df: pd.DataFrame):
    """Duración y distancia promedio por número de pasajeros."""
    st.subheader("⚙️ KPI Operativo — Rendimiento por número de pasajeros")

    col1, col2 = st.columns(2)

    max_duracion = df.loc[df["duracion_promedio_min"].idxmax()]
    max_distancia = df.loc[df["distancia_promedio_km"].idxmax()]

    col1.metric("⏱️ Mayor duración promedio", f"{max_duracion['duracion_promedio_min']:.1f} min",
                f"{int(max_duracion['passenger_count'])} pasajeros")
    col2.metric("📏 Mayor distancia promedio", f"{max_distancia['distancia_promedio_km']:.2f} km",
                f"{int(max_distancia['passenger_count'])} pasajeros")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["passenger_count"].astype(int),
        y=df["duracion_promedio_min"],
        name="Duración (min)",
        marker_color="#00CC96",
    ))
    fig.add_trace(go.Scatter(
        x=df["passenger_count"].astype(int),
        y=df["distancia_promedio_km"],
        name="Distancia (km)",
        marker_color="#AB63FA",
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

    # Composición de viajes (pie chart)
    fig2 = px.pie(df, values="total_viajes", names=df["passenger_count"].astype(int),
                  title="Distribución de viajes por nº de pasajeros",
                  hole=0.4)
    fig2.update_traces(textinfo="percent+label")
    fig2.update_layout(height=380)
    st.plotly_chart(fig2, width="stretch")


def graficar_kpi_demanda(df: pd.DataFrame):
    """Top zonas de recogida con más demanda."""
    st.subheader("📍 KPI Demanda — Viajes por zona de recogida (PULocationID)")

    top_n = st.slider("Mostrar top N zonas", min_value=10, max_value=50, value=20, step=5)

    top = df.head(top_n)
    total_general = df["total_viajes"].sum()
    top_total = top["total_viajes"].sum()

    col1, col2, col3 = st.columns(3)
    col1.metric("🗺️ Zonas con demanda registrada", f"{len(df)}")
    col2.metric("🚕 Total general de viajes", f"{int(total_general):,}")
    col3.metric(f"📊 Top {top_n} concentra", f"{top_total / total_general * 100:.1f}%")

    fig = px.bar(
        top,
        x="total_viajes",
        y=top["PULocationID"].astype(str),
        orientation="h",
        title=f"Top {top_n} zonas de recogida con mayor demanda",
        labels={"total_viajes": "Total Viajes", "PULocationID": "Zona (PULocationID)"},
        color="total_viajes",
        color_continuous_scale="OrRd",
        text_auto=True,
    )
    fig.update_layout(height=500, coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, width="stretch")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    # ── Sidebar ──
    with st.sidebar:
        st.image("https://www.nyc.gov/assets/tlc/images/tlc_logo.png", width=180)
        st.title("🚕 NYC Taxi KPIs")
        st.markdown("**Data Lakehouse** — Medallion Architecture")
        st.divider()

        st.markdown("### 🏗️ Pipeline")
        st.markdown("""
        | Capa | Formato |
        |------|---------|
        | 🟤 Bronze | Parquet |
        | ⚪ Silver | Parquet |
        | 🟡 Gold | CSV |
        """)

        st.divider()
        st.markdown("### 🖥️ Infraestructura")
        st.markdown("""
        - **Hadoop** 3.3.6
        - **Spark** 3.5.0
        - **4 nodos** (ZeroTier)
        - **HDFS** 529 GB
        """)

        st.divider()
        st.caption(f"Datos: NYC TLC Yellow Taxi — Enero 2023")
        st.caption(f"Origen: `{GOLD_BASE}` via WebHDFS")

    # ── Main content ──
    st.title("🚕 Dashboard de KPIs — NYC Yellow Taxi Trip Data")
    st.markdown("**Pipeline Medallion:** Bronze → Silver → Gold | Motor: PySpark sobre YARN | 3,066,766 registros procesados")

    st.divider()

    # Leer los 3 KPIs
    df_financiero = leer_csv_hdfs(KPI_PATHS["financiero"])
    df_operativo  = leer_csv_hdfs(KPI_PATHS["operativo"])
    df_demanda    = leer_csv_hdfs(KPI_PATHS["demanda"])

    # Validación
    if df_financiero.empty or df_operativo.empty or df_demanda.empty:
        st.error("❌ No se pudieron leer los KPIs desde HDFS. Verifica que los datos existan en `/lakehouse/gold/`.")
        st.stop()

    # Tabs para cada KPI
    tab1, tab2, tab3 = st.tabs(["💰 Financiero", "⚙️ Operativo", "📍 Demanda"])

    with tab1:
        graficar_kpi_financiero(df_financiero)
        with st.expander("📄 Ver datos brutos — KPI Financiero"):
            st.dataframe(df_financiero, width="stretch", hide_index=True)

    with tab2:
        graficar_kpi_operativo(df_operativo)
        with st.expander("📄 Ver datos brutos — KPI Operativo"):
            st.dataframe(df_operativo, width="stretch", hide_index=True)

    with tab3:
        graficar_kpi_demanda(df_demanda)
        with st.expander("📄 Ver datos brutos — KPI Demanda"):
            st.dataframe(df_demanda, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
