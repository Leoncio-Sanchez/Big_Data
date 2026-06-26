# Comprobación de Nodos por SSH

## Objetivo

Verificar y configurar el acceso SSH desde el nodo master (`leo`) hacia los 3 workers del cluster ZeroTier, permitiendo arrancar y detener los demonios Hadoop de forma remota sin intervención manual en cada nodo.

---

## 1. Credenciales por Nodo

| Nodo | IP ZeroTier | Usuario SSH | Contraseña |
|:-----|:-----------:|:------------|:-----------|
| `leo` (master) | 10.61.61.105 | `leo` | `leo321` |
| `XUBUNTU` | 10.61.61.12 | `hadoop` | `xubuntu` |
| `DEBIAN` | 10.61.61.65 | `hadoop` | `ADMIN123` |
| 


> **Nota:** Los servicios Hadoop en los workers corren bajo el usuario `hadoop`. El master usa `leo`.

---

## 2. Verificación Rápida de Conectividad

### 2.1 Ping (capa de red ZeroTier)

```bash
# Desde leo, verificar que los 3 workers responden
ping -c 2 -W 1 10.61.61.12   # XUBUNTU
ping -c 2 -W 1 10.61.61.65   # DEBIAN
ping -c 2 -W 1 10.61.61.7    # isait-VirtualBox
```

**Esperado:** 0% packet loss en los 3. Si hay pérdida >50%, revisar ZeroTier.

### 2.2 SSH con contraseña (capa de aplicación)

```bash
# XUBUNTU → password: xubuntu
sshpass -p 'xubuntu' ssh -o StrictHostKeyChecking=no hadoop@10.61.61.12 "hostname"

# DEBIAN → password: ADMIN123
sshpass -p 'ADMIN123' ssh -o StrictHostKeyChecking=no hadoop@10.61.61.65 "hostname"
```

**Esperado:** Retorna el hostname del worker (`XUBUNTU`, `DEBIAN`). Si retorna `Permission denied`, verificar que la contraseña no haya cambiado.

### 2.3 Estado de procesos Java en workers

```bash
# Ver procesos Hadoop/Spark en cada worker
sshpass -p 'xubuntu' ssh -o StrictHostKeyChecking=no hadoop@10.61.61.12 \
  "ps aux | grep -E '(DataNode|NodeManager|NameNode|ResourceManager)' | grep -v grep"

sshpass -p 'ADMIN123' ssh -o StrictHostKeyChecking=no hadoop@10.61.61.65 \
  "ps aux | grep -E '(DataNode|NodeManager|NameNode|ResourceManager)' | grep -v grep"
```

**Esperado:** Al menos `DataNode` corriendo. Si `NodeManager` no aparece, YARN no está levantado en ese worker.

---

## 3. Configurar SSH sin Contraseña (ssh-copy-id)

Para que `start-dfs.sh` y `start-yarn.sh` funcionen sin pedir contraseña en cada worker:

### 3.1 Generar clave SSH en el master (si no existe)

```bash
# Solo si ~/.ssh/id_rsa no existe
ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa
```

### 3.2 Copiar la clave pública a cada worker

```bash
# XUBUNTU
sshpass -p 'xubuntu' ssh-copy-id -o StrictHostKeyChecking=no hadoop@10.61.61.12

# DEBIAN
sshpass -p 'ADMIN123' ssh-copy-id -o StrictHostKeyChecking=no hadoop@10.61.61.65
```

### 3.3 Verificar acceso sin contraseña

```bash
ssh hadoop@10.61.61.12 "hostname"   # Debe retornar XUBUNTU sin pedir password
ssh hadoop@10.61.61.65 "hostname"   # Debe retornar DEBIAN sin pedir password
```

---

## 4. Arranque y Parada Remota de Demonios

### 4.1 DataNode en workers (arranque manual)

```bash
# XUBUNTU
ssh hadoop@10.61.61.12 "export JAVA_HOME=/opt/hadoop/jdk && /opt/hadoop/bin/hdfs --daemon start datanode"

# DEBIAN
ssh hadoop@10.61.61.65 "/opt/hadoop/bin/hdfs --daemon start datanode"
```

### 4.2 DataNode en workers (parada manual)

```bash
ssh hadoop@10.61.61.12 "/opt/hadoop/bin/hdfs --daemon stop datanode"
ssh hadoop@10.61.61.65 "/opt/hadoop/bin/hdfs --daemon stop datanode"
```

### 4.3 NodeManager en workers (si se usa YARN)

```bash
ssh hadoop@10.61.61.12 "/opt/hadoop/bin/yarn --daemon start nodemanager"
ssh hadoop@10.61.61.65 "/opt/hadoop/bin/yarn --daemon start nodemanager"
```

---

## 5. Verificación del Estado del Cluster

### 5.1 Reporte completo de HDFS

```bash
hdfs dfsadmin -report
```

**Revisar:**
- `Live datanodes` debe mostrar 3 nodos (leo, XUBUNTU, DEBIAN)
- `Under replicated blocks` debe ser 0
- `Blocks with corrupt replicas` debe ser 0
- `DFS Remaining` debe mostrar espacio disponible

### 5.2 Estado rápido (solo DataNodes vivos)

```bash
hdfs dfsadmin -report 2>&1 | grep -E "(Live datanodes|Name:|Hostname:|Under replicated)"
```

**Salida esperada:**
```
Live datanodes (3):
Name: 10.61.61.105:9866 (10.61.61.105)
Hostname: leo
Name: 10.61.61.12:9866 (XUBUNTU)
Hostname: XUBUNTU
Name: 10.61.61.65:9866 (DEBIAN)
Hostname: DEBIAN.myguest.virtualbox.org
Under replicated blocks: 0
```

### 5.3 Verificar que los KPIs son legibles

```bash
hdfs dfs -cat /lakehouse/gold/kpi_financiero/*.csv | head -3
hdfs dfs -cat /lakehouse/gold/kpi_operativo/*.csv | head -3
hdfs dfs -cat /lakehouse/gold/kpi_demanda/*.csv | head -3
```

---

## 6. Troubleshooting

### 6.1 `Connection refused` al hacer `hdfs dfs -cat`

**Causa:** El NameNode no está corriendo en `leo:9000`.

**Solución:**
```bash
# En el master (leo)
/opt/hadoop/sbin/start-dfs.sh
```
Si falla por permisos:
```bash
sudo chown -R leo:leo /opt/hadoopdata/hdfs/namenode
sudo chown -R leo:leo /opt/hadoopdata/hdfs/datanode
sudo chown -R leo:leo /opt/hadoop/logs
/opt/hadoop/sbin/start-dfs.sh
```

### 6.2 Worker no aparece en `Live datanodes`

**Causas posibles:**
- El DataNode no está corriendo en el worker
- El worker no puede comunicarse con el NameNode por ZeroTier
- El worker no tiene Java instalado o JAVA_HOME no está configurado

**Diagnóstico:**
```bash
# 1. Probar conectividad
ping -c 2 10.61.61.12

# 2. Probar SSH
ssh hadoop@10.61.61.12 "ps aux | grep DataNode | grep -v grep"

# 3. Si no corre, arrancarlo manualmente
ssh hadoop@10.61.61.12 "export JAVA_HOME=/opt/hadoop/jdk && /opt/hadoop/bin/hdfs --daemon start datanode"

# 4. Revisar logs del worker
ssh hadoop@10.61.61.12 "tail -30 /opt/hadoop/logs/hadoop-hadoop-datanode-*.log"
```

### 6.3 `Permission denied (publickey,password)` en SSH

**Causa:** Las claves SSH no están configuradas o las contraseñas cambiaron.

**Solución:** Repetir el paso 3.2 (`ssh-copy-id`) con las contraseñas actualizadas.

### 6.4 `Under replicated blocks > 0` persistente

**Causa:** Algún DataNode no está corriendo o no tiene espacio.

**Solución:**
```bash
# Verificar espacio en cada worker
ssh hadoop@10.61.61.12 "df -h /opt/hadoopdata"
ssh hadoop@10.61.61.65 "df -h /opt/hadoopdata"

# Forzar re-replicación (si el DataNode ya está corriendo)
hdfs dfsadmin -setBalancerBandwidth 104857600  # 100 MB/s
hdfs balancer -threshold 5
```

### 6.5 `Unable to write in /opt/hadoop/logs` al arrancar Hadoop

**Causa:** El usuario que ejecuta `start-dfs.sh` no es dueño del directorio de logs.

**Solución:**
```bash
sudo chown -R $(whoami):$(whoami) /opt/hadoop/logs
```

### 6.6 `Storage directory does not exist or is not accessible`

**Causa:** El usuario no tiene permisos de escritura sobre los directorios de datos HDFS.

**Solución:**
```bash
sudo chown -R $(whoami):$(whoami) /opt/hadoopdata/hdfs/namenode
sudo chown -R $(whoami):$(whoami) /opt/hadoopdata/hdfs/datanode
```

---

## 7. Script de Verificación Rápida

Guardar como `check_cluster.sh` en el master y ejecutar después de cada reinicio:

```bash
#!/bin/bash
# check_cluster.sh — Verificación rápida del cluster Hadoop ZeroTier

echo "=== 1. Conectividad ZeroTier ==="
for ip in 10.61.61.12 10.61.61.65 10.61.61.7; do
    ping -c 1 -W 1 $ip >/dev/null 2>&1 && echo "  $ip ✅" || echo "  $ip ❌"
done

echo ""
echo "=== 2. SSH a workers ==="
ssh hadoop@10.61.61.12 "hostname" 2>/dev/null && echo "  XUBUNTU ✅" || echo "  XUBUNTU ❌"
ssh hadoop@10.61.61.65 "hostname" 2>/dev/null && echo "  DEBIAN  ✅" || echo "  DEBIAN  ❌"

echo ""
echo "=== 3. DataNodes vivos ==="
hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes"
hdfs dfsadmin -report 2>/dev/null | grep "Hostname:"

echo ""
echo "=== 4. Bloques sub-replicados ==="
hdfs dfsadmin -report 2>/dev/null | grep "Under replicated blocks"
```

---

## 8. Resumen de Configuración Actual

| Parámetro | Valor |
|:----------|:------|
| NameNode RPC | `hdfs://10.61.61.105:9000` |
| NameNode HTTP | `http://10.61.61.105:9870` |
| Workers file | `/opt/hadoop/etc/hadoop/workers` |
| Workers activos | `localhost`, `10.61.61.12`, `10.61.61.65` |
| JAVA_HOME (XUBUNTU) | `/opt/hadoop/jdk` |
| JAVA_HOME (DEBIAN) | Sistema (en PATH) |
| JAVA_HOME (leo) | Sistema (en PATH) |
| Usuario servicios Hadoop | `hadoop` (workers), `leo` (master) |
| Red ZeroTier | `10.61.61.0/24` — interfaz `ztcdchsekn` |
