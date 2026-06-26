#!/usr/bin/env python3
"""
ETAPA 1 - Ingesta a Capa Bronce (Data Lakehouse)
Cluster: 1 Master (NN/RM) + 3 Workers (DN/NM) via ZeroTier
Ejecuta en: Master (usuario hadoop)
"""

import sys
import logging
from pathlib import Path
from typing import Optional

# ─── Config ───
HDFS_URI = "hdfs://leo:9000"             # NameNode RPC real (master = leo)
BRONZE_PATH = "/lakehouse/bronze"
SOURCE_URL = ""                          # URL del dataset online (xlsx) a descargar
LOCAL_TMP = Path("/tmp/online_retail_raw.xlsx")

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("bronze-ingest")

# ─── 1. Verificación de entorno ───
REQUIRED = {
    "requests": "requests",
    "hdfs": "hdfs3",          # pip install hdfs3
    "openpyxl": "openpyxl",   # para leer xlsx si se valida localmente
}

def verificar_entorno() -> bool:
    """Comprueba librerías críticas; imprime comando pip si falta."""
    faltantes = []
    for mod, pkg in REQUIRED.items():
        try:
            __import__(mod)
        except ImportError:
            faltantes.append(pkg)
    if faltantes:
        log.error("Faltan dependencias: %s", ", ".join(faltantes))
        log.error("Instala con: pip install %s", " ".join(faltantes))
        return False
    log.info("Entorno OK: %s", ", ".join(REQUIRED.values()))
    return True

# ─── 2. Descarga de la nube ───
def descargar_data_nube(url: str, destino: Path, timeout: int = 120) -> bool:
    """Descarga streaming con reintentos y verificación de tamaño."""
    import requests
    try:
        log.info("Descargando %s → %s", url, destino)
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", 0))
            written = 0
            with destino.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):  # 1MB chunks
                    f.write(chunk)
                    written += len(chunk)
            log.info("Descarga completa: %d MB (%d bytes)", written // (1<<20), written)
            if total and written != total:
                log.warning("Tamaño inesperado: esperado %d, recibido %d", total, written)
        return True
    except requests.RequestException as e:
        log.exception("Fallo de red descargando: %s", e)
        return False

# ─── 3. Subida a HDFS (Bronce) ───
def subir_a_hdfs(local_file: Path, hdfs_dir: str, hdfs_uri: str) -> bool:
    """Sube archivo a HDFS con replicación 3x y bloque 128MB."""
    from hdfs import InsecureClient
    try:
        client = InsecureClient(hdfs_uri, user="hadoop", timeout=300)
        # Crea directorio bronce si no existe
        client.makedirs(hdfs_dir, permission=0o755)
        remote_path = f"{hdfs_dir}/{local_file.name}"
        log.info("Subiendo a HDFS: %s → %s", local_file, remote_path)
        # write() hace streaming directo a DataNodes (pipeline 3x)
        with local_file.open("rb") as rdr:
            client.write(
                remote_path,
                rdr,
                overwrite=True,
                replication=3,
                blocksize=128 * 1024 * 1024,  # 128MB
                permission=0o644,
            )
        # Verificación rápida
        status = client.status(remote_path)
        log.info("OK HDFS: %s (%d bytes, réplicas=%d, bloques=%d)",
                 remote_path, status["length"], status["replication"], len(status["blockLocations"]))
        return True
    except Exception as e:
        log.exception("Error subiendo a HDFS: %s", e)
        return False

# ─── 4. Orquestador ───
def main() -> int:
    if not verificar_entorno():
        return 1

    # Limpieza previa
    if LOCAL_TMP.exists():
        LOCAL_TMP.unlink()

    if not descargar_data_nube(SOURCE_URL, LOCAL_TMP):
        return 2

    if not subir_a_hdfs(LOCAL_TMP, BRONZE_PATH, HDFS_URI):
        return 3

    # Limpieza local opcional
    LOCAL_TMP.unlink(missing_ok=True)
    log.info("ETAPA 1 finalizada: dato crudo en %s/%s", BRONZE_PATH, LOCAL_TMP.name)
    return 0

if __name__ == "__main__":
    sys.exit(main())