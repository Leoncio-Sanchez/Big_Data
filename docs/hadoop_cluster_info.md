# ☸️ Hadoop Cluster - Reporte Completo de Nodos y Puertos

**Hadoop Version:** 3.3.6  
**Cluster ID:** CID-848d21c9-2c74-40f1-8a36-2632acab5a87  
**Fecha:** 2026-06-25  
**Host maestro:** `leo` (10.61.61.105)

---

## 🖥️ TOPOLOGÍA - NODOS DEL CLUSTER

| # | Hostname | IP | Rol(es) |
|---|----------|----|---------|
| 1 | `leo` | 10.61.61.105 | NameNode + SecondaryNameNode + ResourceManager + DataNode + NodeManager |
| 2 | `XUBUNTU` | 10.61.61.12 | DataNode + NodeManager |
| 3 | `DEBIAN.myguest.virtualbox.org` | 10.61.61.65 | DataNode + NodeManager |
| 4 | `isait-VirtualBox` | 10.61.61.7 | DataNode + NodeManager |

### Storage (HDFS):
- **Capacidad total:** 509.99 GB
- **Capacidad presente:** 147.94 GB
- **DFS usado:** 6.46 GB (4.37%)
- **DFS disponible:** 141.47 GB
- **Factor de replicación:** 3
- **Bloques corruptos:** 0
- **Bloques under-replicated:** 0

### Memoria YARN:
- Cada NodeManager reporta **8192 MB disponibles** (8 GB)
- Contenedores activos: 0

---

## 🔌 MAPA COMPLETO DE PUERTOS POR NODO

### 📍 Nodo 1: `leo` (10.61.61.105) — MAESTRO + WORKER

#### HDFS - NameNode
| Puerto | Protocolo | Binding | Servicio | Propiedad de configuración |
|--------|-----------|---------|----------|---------------------------|
| **9000** | RPC | 10.61.61.105 | NameNode RPC (clientes HDFS) | `fs.defaultFS`, `dfs.namenode.rpc-address` |
| **9870** | HTTP | 0.0.0.0 | NameNode Web UI | `dfs.namenode.http-address` |

#### HDFS - SecondaryNameNode
| Puerto | Protocolo | Binding | Servicio | Propiedad de configuración |
|--------|-----------|---------|----------|---------------------------|
| **9868** | HTTP | 0.0.0.0 | SecondaryNameNode Web UI | `dfs.namenode.secondary.http-address` |

#### HDFS - DataNode (local)
| Puerto | Protocolo | Binding | Servicio | Propiedad de configuración |
|--------|-----------|---------|----------|---------------------------|
| **9866** | TCP | 0.0.0.0 | DataNode data transfer / streaming | `dfs.datanode.address` |
| **9864** | HTTP | 0.0.0.0 | DataNode HTTP Web UI / info server | `dfs.datanode.http.address` |
| **9867** | IPC | 0.0.0.0 | DataNode IPC (inter-process) | `dfs.datanode.ipc.address` |

#### YARN - ResourceManager
| Puerto | Protocolo | Binding | Servicio | Propiedad de configuración |
|--------|-----------|---------|----------|---------------------------|
| **8088** | HTTP | 10.61.61.105 | ResourceManager Web UI | `yarn.resourcemanager.webapp.address` |
| **8030** | RPC | 10.61.61.105 | ApplicationMaster Protocol | `yarn.resourcemanager.scheduler.address` |
| **8031** | RPC | 10.61.61.105 | Resource Tracker (NM heartbeats) | `yarn.resourcemanager.resource-tracker.address` |
| **8032** | RPC | 10.61.61.105 | Client→RM (ApplicationClientProtocol) | `yarn.resourcemanager.address` |
| **8033** | RPC | 10.61.61.105 | RM Admin Protocol | `yarn.resourcemanager.admin.address` |

#### YARN - NodeManager (local)
| Puerto | Protocolo | Binding | Servicio | Propiedad de configuración |
|--------|-----------|---------|----------|---------------------------|
| **8042** | HTTP | 0.0.0.0 | NodeManager Web UI | `yarn.nodemanager.webapp.address` |
| **8040** | RPC | 0.0.0.0 | NM Localizer (localización de recursos) | `yarn.nodemanager.localizer.address` |
| **40135** | RPC | 0.0.0.0 | Container Manager (manejo de contenedores) | `yarn.nodemanager.address` (ephemeral) |
| **13562** | TCP | 0.0.0.0 | MapReduce Shuffle Handler | `mapreduce.shuffle.port` |

---

### 📍 Nodo 2: `XUBUNTU` (10.61.61.12) — WORKER

| Puerto | Protocolo | Binding | Servicio |
|--------|-----------|---------|----------|
| **9866** | TCP | 0.0.0.0 | DataNode data transfer / streaming |
| **9864** | HTTP | 0.0.0.0 | DataNode HTTP Info Server |
| **9867** | IPC | 0.0.0.0 | DataNode IPC |
| **8042** | HTTP | 0.0.0.0 | NodeManager Web UI |
| **39313** | RPC | 0.0.0.0 | NodeManager Container Manager (ephemeral) |
| **13562** | TCP | 0.0.0.0 | MapReduce Shuffle Handler |

---

### 📍 Nodo 3: `DEBIAN.myguest.virtualbox.org` (10.61.61.65) — WORKER

| Puerto | Protocolo | Binding | Servicio |
|--------|-----------|---------|----------|
| **9866** | TCP | 0.0.0.0 | DataNode data transfer / streaming |
| **9864** | HTTP | 0.0.0.0 | DataNode HTTP Info Server |
| **9867** | IPC | 0.0.0.0 | DataNode IPC |
| **8042** | HTTP | 0.0.0.0 | NodeManager Web UI |
| **40657** | RPC | 0.0.0.0 | NodeManager Container Manager (ephemeral) |
| **13562** | TCP | 0.0.0.0 | MapReduce Shuffle Handler |

---

### 📍 Nodo 4: `isait-VirtualBox` (10.61.61.7) — WORKER

| Puerto | Protocolo | Binding | Servicio |
|--------|-----------|---------|----------|
| **9866** | TCP | 0.0.0.0 | DataNode data transfer / streaming |
| **9864** | HTTP | 0.0.0.0 | DataNode HTTP Info Server |
| **9867** | IPC | 0.0.0.0 | DataNode IPC |
| **8042** | HTTP | 0.0.0.0 | NodeManager Web UI |
| **4xxxx** | RPC | 0.0.0.0 | NodeManager Container Manager (ephemeral) |
| **13562** | TCP | 0.0.0.0 | MapReduce Shuffle Handler |

---

## 🗺️ DIAGRAMA DE PUERTOS (vista consolidada)

```
┌─────────────────────────────────────────────────────────┐
│  NODO 1: leo (10.61.61.105) — MAESTRO + WORKER          │
│                                                          │
│  ┌── NameNode ───────────────────────────────────────┐  │
│  │  9000  ← HDFS clients (RPC)                        │  │
│  │  9870  ← Web UI:  http://10.61.61.105:9870         │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌── SecondaryNameNode ───────────────────────────────┐  │
│  │  9868  ← Web UI:  http://10.61.61.105:9868         │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌── ResourceManager ─────────────────────────────────┐  │
│  │  8088  ← Web UI:  http://10.61.61.105:8088         │  │
│  │  8030  ← AppMaster RPC (scheduler)                  │  │
│  │  8031  ← ResourceTracker (NM heartbeats)            │  │
│  │  8032  ← Client RPC (app submission)                │  │
│  │  8033  ← Admin RPC                                  │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌── DataNode (local) ────────────────────────────────┐  │
│  │  9866  ← Data transfer (streaming)                  │  │
│  │  9864  ← Info Server: http://10.61.61.105:9864     │  │
│  │  9867  ← IPC                                        │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌── NodeManager (local) ─────────────────────────────┐  │
│  │  8042  ← Web UI:  http://10.61.61.105:8042         │  │
│  │  8040  ← Localizer RPC                              │  │
│  │  40135 ← Container Manager RPC                      │  │
│  │  13562 ← MapReduce Shuffle                          │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  NODO 2: XUBUNTU (10.61.61.12) — WORKER                 │
│                                                          │
│  ┌── DataNode ────────────────────────────────────────┐  │
│  │  9866  ← Data transfer (streaming)                  │  │
│  │  9864  ← Info Server: http://10.61.61.12:9864      │  │
│  │  9867  ← IPC                                        │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌── NodeManager ─────────────────────────────────────┐  │
│  │  8042  ← Web UI:  http://XUBUNTU:8042               │  │
│  │  39313 ← Container Manager RPC                      │  │
│  │  13562 ← MapReduce Shuffle                          │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  NODO 3: DEBIAN.myguest.virtualbox.org (10.61.61.65)    │
│                                                          │
│  ┌── DataNode ────────────────────────────────────────┐  │
│  │  9866  ← Data transfer (streaming)                  │  │
│  │  9864  ← Info Server: http://10.61.61.65:9864     │  │
│  │  9867  ← IPC                                        │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌── NodeManager ─────────────────────────────────────┐  │
│  │  8042  ← Web UI:  http://DEBIAN.myguest...:8042     │  │
│  │  40657 ← Container Manager RPC                      │  │
│  │  13562 ← MapReduce Shuffle                          │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  NODO 4: isait-VirtualBox (10.61.61.7) — WORKER         │
│                                                          │
│  ┌── DataNode ────────────────────────────────────────┐  │
│  │  9866  ← Data transfer (streaming)                  │  │
│  │  9864  ← Info Server: http://10.61.61.7:9864       │  │
│  │  9867  ← IPC                                        │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌── NodeManager ─────────────────────────────────────┐  │
│  │  8042  ← Web UI:  http://isait-VirtualBox:8042      │  │
│  │  4xxxx ← Container Manager RPC (ephemeral)          │  │
│  │  13562 ← MapReduce Shuffle                          │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## 🔗 URLS DE ACCESO (Web UIs activas)

| Servicio | URL |
|----------|-----|
| NameNode Web UI | http://10.61.61.105:9870 |
| SecondaryNameNode Web UI | http://10.61.61.105:9868 |
| ResourceManager Web UI | http://10.61.61.105:8088 |
| NodeManager Web UI (leo) | http://10.61.61.105:8042 |
| DataNode Info (leo) | http://10.61.61.105:9864 |
| DataNode Info (XUBUNTU) | http://10.61.61.12:9864 |
| DataNode Info (DEBIAN) | http://10.61.61.65:9864 |
| NodeManager Web UI (XUBUNTU) | http://XUBUNTU:8042 |
| NodeManager Web UI (DEBIAN) | http://DEBIAN.myguest.virtualbox.org:8042 |
| DataNode Info (isait-VirtualBox) | http://10.61.61.7:9864 |
| NodeManager Web UI (isait-VirtualBox) | http://isait-VirtualBox:8042 |

---

## 📊 RESUMEN RÁPIDO - TODOS LOS PUERTOS

```
HDFS:
  9000  → NameNode RPC        (clientes HDFS, fs.defaultFS)
  9870  → NameNode Web UI     
  9868  → SecondaryNameNode Web UI
  9866  → DataNode data transfer (streaming, xfer)
  9864  → DataNode HTTP info server
  9867  → DataNode IPC

YARN:
  8088  → ResourceManager Web UI
  8030  → RM Scheduler RPC (AppMaster protocol)
  8031  → RM Resource Tracker (NM heartbeats)
  8032  → RM Client RPC (app submission)
  8033  → RM Admin RPC
  8042  → NodeManager Web UI
  8040  → NM Localizer RPC
  40135 → NM Container Manager (leo - ephemeral)
  39313 → NM Container Manager (XUBUNTU - ephemeral)
  40657 → NM Container Manager (DEBIAN - ephemeral)
  4xxxx → NM Container Manager (isait-VirtualBox - ephemeral)

MapReduce:
  13562 → Shuffle Handler (en cada NodeManager)
```

---

## 🧪 COMANDOS ÚTILES PARA VERIFICAR

```bash
# Reporte de nodos HDFS
hdfs dfsadmin -report

# Nodos YARN
yarn node -list

# Ver todos los puertos en escucha
ss -tlnp | grep -E '9000|9870|986[4-8]|8088|803[0-3]|804[02]|13562|40135|40657|39313|4[0-9]{4}'

# JMX del NameNode (puertos y métricas)
curl -s http://10.61.61.105:9870/jmx | python3 -m json.tool

# JMX del ResourceManager (puertos y nodos activos)
curl -s http://10.61.61.105:8088/jmx | python3 -m json.tool

# Procesos Hadoop corriendo
ps aux | grep -E 'proc_namenode|proc_datanode|proc_resourcemanager|proc_nodemanager|proc_secondarynamenode'
```
