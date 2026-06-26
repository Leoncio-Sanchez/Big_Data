# Fase 4 — KPIs a Capa Gold

## Objetivo

Calcular 3 KPIs de negocio desde los datos limpios en Silver y exportarlos como CSV con cabecera para consumo directo desde Power BI.

**Script:** `/home/leo/Documentos/Big data/procesar_lakehouse.py` — Función `etapa3_gold()`

---

## 1. KPI 1 — Financiero: Ingreso y propina por hora

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

### ¿Qué responde este KPI?

- **¿En qué horas del día se genera más ingreso?** — Identificar horas pico de facturación
- **¿Dónde se deja más propina?** — Horas con mejor propina promedio
- **¿Cuál es el volumen de viajes por hora?** — Distribución horaria de la demanda

### Salida

| Ruta | Formato | Filas |
|------|---------|:-----:|
| `/lakehouse/gold/kpi_financiero/` | CSV con header | 24 (una por hora, 0-23) |

| hora | ingreso_total | propina_promedio | total_viajes |
|:----:|:-------------:|:----------------:|:------------:|
| 0 | $850,234.50 | $3.45 | 12,450 |
| 1 | $620,100.75 | $3.12 | 9,230 |
| ... | ... | ... | ... |
| 17 | $5,620,315.00 | $3.83 | 205,034 |

---

## 2. KPI 2 — Operativo: Rendimiento por pasajero

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

### ¿Qué responde este KPI?

- **¿Los viajes con más pasajeros son más largos?** — Correlación pasajeros vs duración
- **¿Son más eficientes en distancia?** — Distancia promedio por pasajero
- **¿Qué cantidad de pasajeros es más común?** — Distribución de viajes

### Salida

| Ruta | Formato | Filas |
|------|---------|:-----:|
| `/lakehouse/gold/kpi_operativo/` | CSV con header | 8 (passenger_count 1-8) |

| passenger_count | duracion_promedio_min | distancia_promedio_km | total_viajes |
|:---------------:|:---------------------:|:---------------------:|:------------:|
| 1 | 15.2 min | 4.8 km | 1,850,000 |
| 2 | 18.7 min | 6.1 km | 820,000 |
| 3 | 22.1 min | 7.3 km | 180,000 |
| ... | ... | ... | ... |

---

## 3. KPI 3 — Demanda: Viajes por zona de recogida

### Consulta PySpark

```python
kpi_demanda = (df_silver
    .groupBy("PULocationID")
    .agg(F.count("*").alias("total_viajes"))
    .orderBy(F.desc("total_viajes")))
```

### ¿Qué responde este KPI?

- **¿Cuáles son las zonas con mayor demanda de taxis?** — Ranking de PULocationID
- **¿Qué concentración tienen las top zonas?** — Porcentaje de viajes en las zonas más populares

### Salida

| Ruta | Formato | Filas |
|------|---------|:-----:|
| `/lakehouse/gold/kpi_demanda/` | CSV con header | 254 (zonas con al menos 1 viaje) |

| PULocationID | total_viajes |
|:------------:|:------------:|
| 237 | 25,430 |
| 236 | 22,100 |
| 161 | 18,750 |
| ... | ... |

---

## 4. Código de Exportación

Los 3 KPIs se exportan como CSV con cabecera:

```python
def exportar_kpi(kpi_df, ruta):
    (kpi_df
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(ruta))
```

### ¿Por qué CSV y no Parquet?

| Formato | Ventajas | Desventajas |
|---------|----------|-------------|
| **Parquet** | Columnar, comprimido, schema nativo | Power BI requiere config extra |
| **CSV** | Universal, Power BI lo lee directo | Más peso, sin compresión |

Para Power BI, CSV es más simple y directo.

---

## 5. Tiempos de Ejecución

```
ETAPA                         | TIEMPO   | RESULTADO
══════════════════════════════╪══════════╪══════════════════════════════════════
1. Creación SparkSession      | ~2 min   | ✓ YARN, 3 executors × 4GB/2 cores
2. ETAPA 2: Bronze → Silver   | ~2.5 min | ✓ 2,906,607 registros
3. KPI 1: Financiero          | ~29s     | ✓ 24 filas
4. KPI 2: Operativo           | ~14s     | ✓ 8 filas
5. KPI 3: Demanda             | ~26s     | ✓ 254 filas
──────────────────────────────┴──────────┴──────────────────────────────────────
  TOTAL                       | ~3.5 min | ✓ PIPELINE COMPLETO
```

---

## 6. Resultados Finales del Pipeline

| Indicador | Valor |
|-----------|-------|
| Registros crudos (Bronze) | 3,066,766 |
| Registros limpios (Silver) | 2,906,607 |
| Registros descartados | 160,159 (5.2%) |
| Tamaño Bronze | 45.5 MB (×3 réplicas = 136.4 MB) |
| Tiempo total del pipeline | ~3.5 minutos |
| Workers utilizados | 3 × (4 GB RAM, 2 cores) |

---

## 7. Estructura Final en HDFS

```
/lakehouse/
├── bronze/
│   └── yellow_tripdata_2023-01.parquet   45.5 MB × 3
│
├── silver/
│   └── taxis_limpio/
│       ├── PULocationID=1/
│       ├── ...
│       └── PULocationID=265/
│
└── gold/
    ├── kpi_financiero/     (24 filas — ingreso/propina por hora)
    ├── kpi_operativo/      (8 filas — duración/distancia por pasajero)
    └── kpi_demanda/        (254 filas — viajes por zona)
```
