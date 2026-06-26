# Fase 2 — Ingesta a Capa Bronce (Bronze)

## Objetivo

Descargar el dataset Yellow Taxi Trips de NYC (Enero 2023) desde una URL pública y subirlo como datos crudos e inmutables a la capa Bronze en HDFS.

**Script:** `/home/leo/Documentos/Big data/bronze_ingest.py`

---

## 1. Dataset

| Propiedad | Valor |
|-----------|-------|
| Fuente | NYC Taxi & Limousine Commission |
| URL | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet` |
| Formato | Parquet (columnar, comprimido) |
| Tamaño | ~45 MB (45.5 MB exactos) |
| Registros | ~3,066,766 |
| Réplicas en HDFS | 3 (x 45.5 MB = 136.4 MB total) |

---

## 2. Estructura del Script

### 2.1 Configuración inicial

```python
HDFS_URI = "http://leo:9870"             # WebHDFS via HTTP (no RPC)
BRONZE_PATH = "/lakehouse/bronze"         # Directorio en HDFS
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
LOCAL_TMP = Path("/tmp/yellow_tripdata_2023-01.parquet")
```

### 2.2 Función: verificar_entorno()

```python
REQUIRED = {"requests": "requests", "hdfs": "hdfs"}

def verificar_entorno() -> bool:
    for mod, pkg in REQUIRED.items():
        try:
            __import__(mod)
        except ImportError:
            faltantes.append(pkg)
    # Si faltan, muestra comando pip y retorna False
```

### 2.3 Función: descargar_data_nube()

```python
def descargar_data_nube(url, destino, timeout=120) -> bool:
    import requests
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with destino.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):  # 1MB chunks
                f.write(chunk)
```

Descarga en streaming (chunks de 1MB) para no saturar RAM con el archivo completo.

### 2.4 Función: subir_a_hdfs()

```python
def subir_a_hdfs(local_file, hdfs_dir, hdfs_uri) -> bool:
    client = InsecureClient(hdfs_uri, user="hadoop", timeout=300)
    client.makedirs(hdfs_dir, permission="755")  # String octal!
    remote_path = f"{hdfs_dir}/{local_file.name}"
    with local_file.open("rb") as rdr:
        client.write(remote_path, rdr, overwrite=True, replication=3,
                     blocksize=128 * 1024 * 1024, permission="644")  # String octal!
```

Usa WebHDFS (`hdfs.InsecureClient`) con:
- Puerto **9870** (HTTP, no 9000 que es RPC binario)
- Usuario `hadoop` (el que creó los directorios)
- Replicación 3 (tolerancia a fallos)
- Bloques de 128MB

---

## 3. Bugs Encontrados y Corregidos

### Bug 1: Librería incorrecta (hdfs3 vs hdfs)

```python
# ❌ Versión original: hdfs3 (RPC nativo, requiere librería C++)
from hdfs3 import HDFSFile

# ✅ Versión corregida: hdfs (WebHDFS via HTTP)
from hdfs import InsecureClient
```

**Causa:** `hdfs3` usa el protocolo RPC binario de Hadoop (puerto 9000), que requiere librerías nativas C++. Desde Python puro (sin JVM), no funciona. La librería `hdfs` usa WebHDFS via HTTP (puerto 9870).

**Referencia en código:** `bronze_ingest.py:26` — Cambiar de `"hdfs": "hdfs3"` a `"hdfs": "hdfs"`.

### Bug 2: Puerto incorrecto

```python
# ❌ Puerto 9000 (RPC binario, solo para Java/Spark)
client = InsecureClient("http://leo:9000", user="hadoop")

# ✅ Puerto 9870 (HTTP, accesible desde Python)
client = InsecureClient("http://leo:9870", user="hadoop")
```

**Diferencia clave:**
| Puerto | Protocolo | Uso |
|:------:|-----------|-----|
| 9000 | RPC binario | Clientes Java/Spark |
| 9870 | HTTP REST | WebHDFS, navegador, Python |

### Bug 3: Permiso octal como entero

```python
# ❌ Entero Python (falla)
client.makedirs(hdfs_dir, permission=0o755)
client.write(..., permission=0o644)

# ✅ String octal (funciona)
client.makedirs(hdfs_dir, permission="755")
client.write(..., permission="644")
```

**Error original:**
```
Invalid value for webhdfs parameter "permission":
Failed to parse "493" as a radix-8 short integer.
```

La librería `hdfs` espera strings con el valor octal, no enteros Python.

### Bug 4: Dataset incorrecto

```python
# ❌ Dataset original (Online Retail XLSX)
SOURCE_URL = ""  # URL de UCI
LOCAL_TMP = Path("/tmp/online_retail_raw.xlsx")

# ✅ Dataset corregido (Yellow Taxi NYC Parquet)
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
LOCAL_TMP = Path("/tmp/yellow_tripdata_2023-01.parquet")
```

---

## 4. Instalación de Dependencias

```bash
pip install --break-system-packages hdfs requests
```

---

## 5. Ejecución

```bash
cd "/home/leo/Documentos/Big data"
python3 bronze_ingest.py
```

### Logs esperados

```
2026-06-25 | INFO     | Descargando yellow_tripdata_2023-01.parquet → /tmp/...
2026-06-25 | INFO     | Descarga completa: 45 MB (47673370 bytes)
2026-06-25 | INFO     | Subiendo a HDFS: → /lakehouse/bronze/yellow_tripdata_2023-01.parquet
2026-06-25 | INFO     | ETAPA 1 finalizada ✅
```

---

## 6. Verificación

```bash
hdfs dfs -ls /lakehouse/bronze/
# -rw-r--r--   3 hadoop supergroup   47673370  /lakehouse/bronze/yellow_tripdata_2023-01.parquet
```

Desde el navegador: http://10.61.61.105:9870/explorer.html#/lakehouse/bronze/

---

## 7. Estructura Resultante

```
/lakehouse/
└── bronze/
    └── yellow_tripdata_2023-01.parquet   45.5 MB × réplica 3
```

---

## 8. Resumen de Problemas y Soluciones

| # | Problema | Causa | Solución |
|:-:|----------|-------|----------|
| 1 | `hdfs3` no funciona | Librería incompatible con Python puro | Usar `hdfs` (WebHDFS) |
| 2 | Connection refused en puerto 9000 | Usar puerto RPC en vez de HTTP | Usar puerto 9870 |
| 3 | `Failed to parse "493" as radix-8` | Permiso como entero en vez de string | Usar `"755"` en vez de `0o755` |
| 4 | Dataset incorrecto | URL apuntaba a Online Retail XLSX | Cambiar a Yellow Taxi NYC Parquet |
