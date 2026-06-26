# Fase 1 — Diagnóstico y Reparación del Cluster

## Objetivo

Detectar por qué el nodo `isait-VirtualBox` (10.61.61.7) no aparecía en YARN, diagnosticar la causa raíz y repararlo para recuperar el 100% del cluster.

---

## 1. Detección del Problema

Al verificar el estado del cluster después de la instalación:

```bash
leo@leo:~$ yarn node -list
Total Nodes:3
         Node-Id             Node-State    Node-Http-Address
       leo:40135                RUNNING    leo:8042
   XUBUNTU:39313                RUNNING    XUBUNTU:8042
DEBIAN...:40657                 RUNNING    DEBIAN...:8042
```

**Problema:** `isait-VirtualBox` no aparece. Cluster operando al 75%.

```bash
hdfs dfsadmin -report | grep -i isait
# Sin resultados — el DataNode tampoco está registrado
```

---

## 2. Diagnóstico Paso a Paso

### 2.1 Verificar conectividad de red

```bash
ping -c 3 10.61.61.7
# ✅ 3/3 paquetes respondidos (latencia 17-237ms)
```

La VPN ZeroTier funciona. El nodo isait está vivo en la red.

### 2.2 Verificar servicios en isait

```bash
# ¿NodeManager responde?
curl -s --connect-timeout 5 http://10.61.61.7:8042/node
# ❌ NO_RESPONDE — NodeManager no escucha en el puerto HTTP
```

### 2.3 Acceso SSH a isait

```bash
sshpass -p 'Isait@2001' ssh isait@10.61.61.7
# Usuario: isait | Password: Isait@2001
```

### 2.4 Revisar procesos Hadoop en isait

```bash
ps aux | grep -E 'datanode|nodemanager'
# 2873 isait  ... proc_nodemanager  ← Solo NodeManager, como usuario isait
# DataNode no está corriendo
```

### 2.5 Analizar logs del ResourceManager (leo)

```bash
grep -i "10.61.61.7\|isait" /opt/hadoop/logs/hadoop-hadoop-resourcemanager-leo.log | tail -30
```

**Salida clave:**
```
20:03:43 INFO  ... NodeManager from isait-VirtualBox(cmPort: 44621 httpPort: 8042)
                 registered with capability: <memory:8192, vCores:8>
20:03:43 INFO  ... isait-VirtualBox:44621 Node Transitioned from NEW to UNHEALTHY
20:03:43 ERROR ... Attempting to remove non-existent node isait-VirtualBox:44621
```

**Interpretación:** El ResourceManager **sí recibía** heartbeats de isait, pero lo marcaba como `UNHEALTHY` inmediatamente.

### 2.6 Analizar logs del NodeManager (isait)

```bash
tail -100 /opt/hadoop/logs/hadoop-isait-nodemanager*.log | grep -E 'ERROR|WARN'
```

**Causa raíz confirmada:**
```
ERROR ... LocalDirsHandlerService: Most of the disks failed.
       1/1 log-dirs have errors: [/opt/hadoop/logs/userlogs:
       Directory is not writable: /opt/hadoop/logs/userlogs]
```

### 2.7 Verificar permisos

```bash
ls -la /opt/hadoop/logs/
# drwxr-xr-x  2 hadoop hadoop  /opt/hadoop/logs/userlogs    ← solo hadoop puede escribir
# drwxrwxrwx  3 hadoop hadoop  /opt/hadoop/logs/             ← world-writable
```

### 2.8 Resumen de causa raíz

| Componente | Dueño esperado | Proceso corriendo como | ¿Coinciden? |
|-----------|:---:|:---:|:---:|
| Directorios Hadoop | `hadoop:hadoop` | — | — |
| NodeManager | `hadoop` | `isait` | ❌ |
| DataNode | `hadoop` | (detenido) | ❌ |

**Problema de fondo:** El NodeManager se ejecutaba como `isait`, pero los directorios pertenecen a `hadoop`. El directorio `userlogs` (`drwxr-xr-x`) solo permitía escritura al dueño (`hadoop`). `isait` no podía escribir → YARN marcaba el nodo como `UNHEALTHY`.

---

## 3. Reparación

### Paso 1: Arreglar permisos de userlogs

```bash
echo 'Isait@2001' | sudo -S chmod 777 /opt/hadoop/logs/userlogs
```

### Paso 2: Matar el NodeManager viejo (corriendo como isait)

```bash
echo 'Isait@2001' | sudo -S pkill -f proc_nodemanager
```

### Paso 3: Iniciar DataNode como usuario hadoop

```bash
echo 'Isait@2001' | sudo -S -u hadoop /opt/hadoop/bin/hdfs --daemon start datanode
```

### Paso 4: Iniciar NodeManager como usuario hadoop

```bash
echo 'Isait@2001' | sudo -S -u hadoop /opt/hadoop/bin/yarn --daemon start nodemanager
```

### Paso 5: Verificar procesos

```bash
ps aux | grep -E 'proc_datanode|proc_nodemanager' | grep -v grep
# hadoop  3698  ... -Dproc_datanode    ... DataNode      ← como hadoop ✅
# hadoop  3846  ... -Dproc_nodemanager ... NodeManager   ← como hadoop ✅
```

---

## 4. Verificación Final

Desde el master (leo):

```bash
leo@leo:~$ yarn node -list
Total Nodes:4
         Node-Id             Node-State    Node-Http-Address
       leo:40135                RUNNING    leo:8042
   XUBUNTU:39313                RUNNING    XUBUNTU:8042
DEBIAN...:40657                 RUNNING    DEBIAN...:8042
isait-VirtualBox:36905          RUNNING    isait-VirtualBox:8042   ✅
```

```bash
hdfs dfsadmin -report
# 4 DataNodes activos ✅
# Capacidad: 529 GB total, 149 GB disponible
# Bloques corruptos: 0
```

---

## 5. Estado del Cluster Post-Reparación

| Indicador | Estado |
|-----------|--------|
| Nodos YARN | 4/4 RUNNING ✅ |
| DataNodes HDFS | 4/4 activos ✅ |
| Capacidad HDFS | 529 GB total, 149 GB disponible |
| Bloques corruptos | 0 ✅ |

---

## 6. Lección Aprendida

| Problema | Causa | Solución |
|----------|-------|----------|
| isait no aparece en YARN | NodeManager corriendo como `isait` sin permisos de escritura en `userlogs` | Servicios Hadoop deben ejecutarse como `hadoop` (dueño de los directorios) + `chmod 777` en `userlogs` |
