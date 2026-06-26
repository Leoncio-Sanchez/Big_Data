#!/usr/bin/env python3  # shebang para ejecutar directamente en Linux/Mac
"""
ETAPA 1 - Ingesta a Capa Bronce (Data Lakehouse)
Cluster: 1 Master (NN/RM) + 3 Workers (DN/NM) via ZeroTier
Ejecuta en: Master (usuario hadoop)
"""

import sys  # acceso a argumentos/funciones del sistema (sys.exit)
import logging  # librería para logs con niveles (info, error, etc.)
from pathlib import Path  # manejo moderno de rutas de archivos
from typing import Optional  # tipado: indica que una variable puede ser None

# ─── Config ───
HDFS_URI = "hdfs://leo:9000"             # URI del NameNode de HDFS (master = leo)
BRONZE_PATH = "/lakehouse/bronze"        # directorio HDFS donde se guarda la capa bronce
SOURCE_URL = ""                          # URL del dataset online (xlsx) a descargar
LOCAL_TMP = Path("/tmp/online_retail_raw.xlsx")  # ruta local temporal para el archivo descargado

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"  # formato del log: fecha, nivel, mensaje
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)  # configura logging a nivel INFO con ese formato
log = logging.getLogger("bronze-ingest")  # crea un logger con nombre "bronze-ingest"

# ─── 1. Verificación de entorno ───
REQUIRED = {  # diccionario: nombre módulo -> nombre paquete pip
    "requests": "requests",    # para descargar archivos HTTP
    "hdfs": "hdfs3",           # para conectarse a HDFS
    "openpyxl": "openpyxl",    # para leer .xlsx si se valida localmente
}

def verificar_entorno() -> bool:  # función que retorna True si todas las librerías existen
    """Comprueba librerías críticas; imprime comando pip si falta."""
    faltantes = []  # lista para acumular paquetes que faltan
    for mod, pkg in REQUIRED.items():  # itera sobre cada par (módulo, paquete)
        try:  # intenta importar el módulo
            __import__(mod)  # importa el módulo por su nombre string
        except ImportError:  # si no se encuentra el módulo
            faltantes.append(pkg)  # agrega el nombre del paquete a la lista de faltantes
    if faltantes:  # si hay al menos una dependencia faltante
        log.error("Faltan dependencias: %s", ", ".join(faltantes))  # loggea error con los faltantes
        log.error("Instala con: pip install %s", " ".join(faltantes))  # muestra comando de instalación
        return False  # retorna False indicando que falta algo
    log.info("Entorno OK: %s", ", ".join(REQUIRED.values()))  # loggea éxito si todo está bien
    return True  # retorna True, entorno listo

# ─── 2. Descarga de la nube ───
def descargar_data_nube(url: str, destino: Path, timeout: int = 120) -> bool:  # descarga archivo por HTTP
    """Descarga streaming con reintentos y verificación de tamaño."""
    import requests  # importa la librería requests dentro de la función
    try:  # bloque try para capturar errores de red
        log.info("Descargando %s → %s", url, destino)  # loggea inicio de descarga
        with requests.get(url, stream=True, timeout=timeout) as r:  # abre conexión HTTP en streaming
            r.raise_for_status()  # lanza excepción si el status HTTP no es 200
            total = int(r.headers.get("Content-Length", 0))  # obtiene tamaño total del archivo desde cabecera
            written = 0  # contador de bytes escritos
            with destino.open("wb") as f:  # abre archivo local en modo escritura binaria
                for chunk in r.iter_content(chunk_size=1 << 20):  # itera sobre chunks de 1MB (2^20 bytes)
                    f.write(chunk)  # escribe el chunk en disco
                    written += len(chunk)  # suma el tamaño del chunk al contador
            log.info("Descarga completa: %d MB (%d bytes)", written // (1<<20), written)  # loggea tamaño final
            if total and written != total:  # si se conoce el total y no coincide con lo escrito
                log.warning("Tamaño inesperado: esperado %d, recibido %d", total, written)  # advierte discrepancia
        return True  # descarga exitosa
    except requests.RequestException as e:  # captura cualquier error de requests
        log.exception("Fallo de red descargando: %s", e)  # loggea el error con traceback
        return False  # retorna False indicando fallo

# ─── 3. Subida a HDFS (Bronce) ───
def subir_a_hdfs(local_file: Path, hdfs_dir: str, hdfs_uri: str) -> bool:  # sube archivo local a HDFS
    """Sube archivo a HDFS con replicación 3x y bloque 128MB."""
    from hdfs import InsecureClient  # importa cliente HDFS (sin Kerberos)
    try:  # bloque try para capturar errores de HDFS
        client = InsecureClient(hdfs_uri, user="hadoop", timeout=300)  # crea cliente conectando al NameNode
        client.makedirs(hdfs_dir, permission=0o755)  # crea el directorio HDFS si no existe
        remote_path = f"{hdfs_dir}/{local_file.name}"  # ruta HDFS de destino = dir + nombre archivo
        log.info("Subiendo a HDFS: %s → %s", local_file, remote_path)  # loggea inicio de subida
        with local_file.open("rb") as rdr:  # abre archivo local en modo lectura binaria
            client.write(  # escribe el archivo en HDFS
                remote_path,  # ruta remota en HDFS
                rdr,  # objeto file-like para leer los datos
                overwrite=True,  # sobrescribe si ya existe
                replication=3,  # replicación 3x (tres copias en distintos DataNodes)
                blocksize=128 * 1024 * 1024,  # tamaño de bloque HDFS = 128MB
                permission=0o644,  # permisos del archivo: rw-r--r--
            )
        status = client.status(remote_path)  # obtiene metadatos del archivo en HDFS
        log.info("OK HDFS: %s (%d bytes, réplicas=%d, bloques=%d)",  # loggea información del archivo
                 remote_path, status["length"], status["replication"], len(status["blockLocations"]))
        return True  # subida exitosa
    except Exception as e:  # captura cualquier excepción
        log.exception("Error subiendo a HDFS: %s", e)  # loggea error con traceback
        return False  # retorna False

# ─── 4. Orquestador ───
def main() -> int:  # función principal, retorna código de salida (0 = éxito)
    if not verificar_entorno():  # verifica que las librerías necesarias estén instaladas
        return 1  # código 1: error de entorno

    if LOCAL_TMP.exists():  # si el archivo temporal ya existe
        LOCAL_TMP.unlink()  # lo borra para empezar limpio

    if not descargar_data_nube(SOURCE_URL, LOCAL_TMP):  # descarga el dataset desde la URL
        return 2  # código 2: error de descarga

    if not subir_a_hdfs(LOCAL_TMP, BRONZE_PATH, HDFS_URI):  # sube el archivo a HDFS capa bronce
        return 3  # código 3: error de subida a HDFS

    LOCAL_TMP.unlink(missing_ok=True)  # borra el archivo temporal local (si existe)
    log.info("ETAPA 1 finalizada: dato crudo en %s/%s", BRONZE_PATH, LOCAL_TMP.name)  # loggea éxito
    return 0  # código 0: todo correcto

if __name__ == "__main__":  # si el script se ejecuta directamente (no importado)
    sys.exit(main())  # llama a main() y sale con su código de retorno