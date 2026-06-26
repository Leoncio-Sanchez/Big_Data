# Guía de Instalación — Hadoop 3.3.6 + ZeroTier

**Cluster:** 4 nodos | **OS:** Linux | **Usuario:** leo

---

## Índice

1. [Requisitos Previos](#1-requisitos-previos)
2. [ZeroTier: Red Privada Virtual](#2-zerotier-red-privada-virtual)
3. [Hadoop: Instalación y Configuración](#3-hadoop-instalación-y-configuración)
4. [Arranque del Cluster](#4-arranque-del-cluster)
5. [Verificación](#5-verificación)
6. [Troubleshooting](#6-troubleshooting)

---

## 1. Requisitos Previos

### 1.1 Hardware

| Nodo | Hostname | IP ZeroTier | RAM | Disco | Rol |
|------|----------|-------------|-----|-------|-----|
| 1 | `leo` | 10.61.61.105 | 8GB+ | 50GB+ | NameNode + ResourceManager + DataNode |
| 2 | `XUBUNTU` | 10.61.61.12 | 8GB+ | 50GB+ | DataNode + NodeManager |
| 3 | `DEBIAN` | 10.61.61.65 | 8GB+ | 50GB+ | DataNode + NodeManager |
| 4 | `isait-VirtualBox` | 10.61.61.7 | 8GB+ | 50GB+ | DataNode + NodeManager |

### 1.2 Software necesario en cada nodo

```bash
# Java 8 u 11 (obligatorio para Hadoop 3.x)
sudo apt update && sudo apt install -y openjdk-11-jdk

# SSH (servidor + cliente)
sudo apt install -y openssh-server openssh-client

# Python 3 (para scripts auxiliares)
sudo apt install -y python3 python3-pip

# curl, wget, rsync
sudo apt install -y curl wget rsync
```

### 1.3 Verificar Java

```bash
java -version
# openjdk version "11.0.x"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

---

## 2. ZeroTier: Red Privada Virtual

### 2.1 Instalar ZeroTier en cada nodo

```bash
curl -s https://install.zerotier.com | sudo bash
```

### 2.2 Unir cada nodo a la red

```bash
# En CADA nodo (master + workers)
sudo zerotier-cli join <NETWORK_ID>
```

### 2.3 Autorizar nodos desde ZeroTier Central

1. Ir a https://my.zerotier.com
2. Seleccionar la red
3. Autorizar (check) los 4 miembros: leo, XUBUNTU, DEBIAN, isait-VirtualBox
4. Opcional: asignar IPs estáticas (ej: 10.61.61.105, .12, .65, .7)

### 2.4 Verificar conectividad

```bash
# En cada nodo, probar ping a los demás
ping 10.61.61.105   # leo
ping 10.61.61.12    # XUBUNTU
ping 10.61.61.65    # DEBIAN
ping 10.61.61.7     # isait-VirtualBox

# Ver miembros de la red
sudo zerotier-cli listnetworks
```

### 2.5 (Opcional) Resolución de nombres via /etc/hosts

En cada nodo, agregar:

```bash
sudo tee -a /etc/hosts << 'EOF'

# ZeroTier cluster
10.61.61.105  leo
10.61.61.12   XUBUNTU
10.61.61.65   DEBIAN
10.61.61.7    isait-VirtualBox
EOF
```

---

## 3. Hadoop: Instalación y Configuración

### 3.1 Descargar Hadoop (solo en el master)

```bash
cd /tmp
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar -xzf hadoop-3.3.6.tar.gz -C /usr/local
sudo mv /usr/local/hadoop-3.3.6 /usr/local/hadoop
sudo chown -R leo:leo /usr/local/hadoop
```

### 3.2 Configurar variables de entorno

Agregar a `~/.bashrc` (o `~/.profile`) del usuario `leo`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

export HDFS_NAMENODE_USER=leo
export HDFS_DATANODE_USER=leo
export HDFS_SECONDARYNAMENODE_USER=leo
export YARN_RESOURCEMANAGER_USER=leo
export YARN_NODEMANAGER_USER=leo
```

Aplicar cambios:

```bash
source ~/.bashrc
```

### 3.3 SSH sin contraseña (key-based)

```bash
# Generar clave SSH en leo (master)
ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa

# Copiar clave a sí mismo (para start-all.sh local)
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# Copiar clave a cada worker
ssh-copy-id leo@XUBUNTU
ssh-copy-id leo@DEBIAN
ssh-copy-id leo@isait-VirtualBox

# Verificar acceso sin contraseña
ssh XUBUNTU "hostname"
ssh DEBIAN "hostname"
ssh isait-VirtualBox "hostname"
```

### 3.4 Archivos de Configuración de Hadoop

#### `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_LOG_DIR=$HADOOP_HOME/logs
export HDFS_NAMENODE_USER=leo
export HDFS_DATANODE_USER=leo
export HDFS_SECONDARYNAMENODE_USER=leo
export YARN_RESOURCEMANAGER_USER=leo
export YARN_NODEMANAGER_USER=leo
export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no"
```

#### `core-site.xml`

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://10.61.61.105:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/usr/local/hadoop/tmp</value>
    </property>
</configuration>
```

#### `hdfs-site.xml`

```xml
<configuration>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:9870</value>
    </property>
    <property>
        <name>dfs.namenode.rpc-address</name>
        <value>10.61.61.105:9000</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.blocksize</name>
        <value>134217728</value>
        <description>128MB</description>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/usr/local/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/usr/local/hadoop/data/datanode</value>
    </property>
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>true</value>
    </property>
</configuration>
```

#### `yarn-site.xml`

```xml
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>10.61.61.105</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>10.61.61.105:8032</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>10.61.61.105:8030</value>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>10.61.61.105:8031</value>
    </property>
    <property>
        <name>yarn.resourcemanager.admin.address</name>
        <value>10.61.61.105:8033</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>10.61.61.105:8088</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>8192</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>8192</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>1024</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>
```

#### `mapred-site.xml`

```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.shuffle.port</name>
        <value>13562</value>
    </property>
</configuration>
```

#### `workers` (lista de nodos workers)

```
XUBUNTU
DEBIAN
isait-VirtualBox
```

### 3.5 Distribuir Hadoop a los workers

```bash
# Desde el master (leo)
cd /usr/local
tar czf hadoop.tar.gz hadoop/

# Enviar a cada worker y extraer
for node in XUBUNTU DEBIAN isait-VirtualBox; do
    scp hadoop.tar.gz leo@$node:/tmp/
    ssh leo@$node "sudo tar -xzf /tmp/hadoop.tar.gz -C /usr/local && sudo chown -R leo:leo /usr/local/hadoop"
done
rm hadoop.tar.gz
```

### 3.6 Configurar variables de entorno en workers

En cada worker, agregar a `~/.bashrc` del usuario `leo`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

export HDFS_NAMENODE_USER=leo
export HDFS_DATANODE_USER=leo
export YARN_NODEMANAGER_USER=leo
```

```bash
source ~/.bashrc
```

---

## 4. Arranque del Cluster

### 4.1 Formatear el NameNode (solo la primera vez)

```bash
# En el master (leo)
hdfs namenode -format
```

### 4.2 Iniciar HDFS

```bash
# En el master (leo)
$HADOOP_HOME/sbin/start-dfs.sh
```

Esto inicia:
- NameNode en leo (puerto 9870 web, 9000 RPC)
- SecondaryNameNode en leo (puerto 9868)
- DataNode en cada worker (puertos 9866, 9864, 9867)

### 4.3 Iniciar YARN

```bash
# En el master (leo)
$HADOOP_HOME/sbin/start-yarn.sh
```

Esto inicia:
- ResourceManager en leo (puerto 8088 web)
- NodeManager en cada worker (puerto 8042 web, 13562 shuffle)

### 4.4 Iniciar todo de una vez

```bash
$HADOOP_HOME/sbin/start-all.sh
```

### 4.5 Detener el cluster

```bash
$HADOOP_HOME/sbin/stop-all.sh
```

---

## 5. Verificación

### 5.1 Procesos activos

```bash
# En el master
jps
# Deberías ver:
# NameNode
# SecondaryNameNode
# ResourceManager
# DataNode
# NodeManager

# En cada worker
jps
# Deberías ver:
# DataNode
# NodeManager
```

### 5.2 Web UIs

| Servicio | URL |
|----------|-----|
| NameNode Web UI | http://10.61.61.105:9870 |
| ResourceManager YARN | http://10.61.61.105:8088 |
| DataNode leo | http://10.61.61.105:9864 |
| DataNode XUBUNTU | http://10.61.61.12:9864 |
| DataNode DEBIAN | http://10.61.61.65:9864 |
| DataNode isait-VirtualBox | http://10.61.61.7:9864 |
| NodeManager leo | http://10.61.61.105:8042 |

### 5.3 Comandos de verificación

```bash
# Reporte HDFS completo
hdfs dfsadmin -report

# Listar nodos YARN
yarn node -list

# Crear directorio de prueba
hdfs dfs -mkdir /test

# Subir archivo de prueba
echo "Hola Hadoop" | hdfs dfs -put - /test/saludo.txt

# Leer archivo de prueba
hdfs dfs -cat /test/saludo.txt

# Ver estructura
hdfs dfs -ls -R /
```

---

## 6. Puertos Clave del Cluster

| Puerto | Servicio | Nodo | Protocolo |
|--------|----------|------|-----------|
| 9000 | NameNode RPC | leo | TCP (binario) |
| 9870 | NameNode Web UI | leo | HTTP |
| 9868 | SecondaryNameNode UI | leo | HTTP |
| 9866 | DataNode transfer | todos | TCP |
| 9864 | DataNode HTTP | todos | HTTP |
| 8088 | ResourceManager UI | leo | HTTP |
| 8032 | ResourceManager Client RPC | leo | TCP |
| 8030 | ResourceManager Scheduler | leo | TCP |
| 8031 | ResourceManager Tracker | leo | TCP |
| 8042 | NodeManager UI | todos | HTTP |
| 13562 | MapReduce Shuffle | todos | TCP |

---

## 7. Troubleshooting

### 7.1 ZeroTier

| Problema | Causa | Solución |
|----------|-------|----------|
| No hay ping entre nodos | Firewall o ZeroTier no autorizado | `sudo ufw allow 9993` y autorizar en my.zerotier.com |
| ZeroTier service no inicia | Service no habilitado | `sudo systemctl enable --now zerotier-one` |
| IP no asignada | No autorizado en Central | Revisar https://my.zerotier.com |

### 7.2 Hadoop

| Problema | Causa | Solución |
|----------|-------|----------|
| NameNode no arranca | Formato no realizado | `hdfs namenode -format` |
| DataNode no conecta | IP incorrecta en core-site.xml | Verificar `fs.defaultFS` apunte a la IP ZeroTier |
| SafeMode activo | NameNode en recuperación | `hdfs dfsadmin -safemode leave` |
| Disk space insuficiente | Nodo sin espacio | Revisar `df -h` y limpiar `/tmp` |
| Connection refused | Puerto no abierto | `ss -tlnp | grep <PUERTO>` y verificar servicio activo |

### 7.3 Logs

```bash
# Logs de Hadoop (en el master)
$HADOOP_HOME/logs/
ls $HADOOP_HOME/logs/hadoop-leo-namenode-*.log
ls $HADOOP_HOME/logs/hadoop-leo-datanode-*.log
ls $HADOOP_HOME/logs/yarn-leo-resourcemanager-*.log

# Ver logs en tiempo real
tail -f $HADOOP_HOME/logs/hadoop-leo-namenode-*.log
```

### 7.4 Reseteo completo del cluster

```bash
# Detener todo
$HADOOP_HOME/sbin/stop-all.sh

# Eliminar datos (cuidado: borra TODO)
rm -rf /usr/local/hadoop/data /usr/local/hadoop/tmp /usr/local/hadoop/logs

# Re-crear directorios
mkdir -p /usr/local/hadoop/data/namenode /usr/local/hadoop/data/datanode /usr/local/hadoop/tmp

# Formatear
hdfs namenode -format

# Iniciar
$HADOOP_HOME/sbin/start-all.sh
```

---

*Documentación generada el 25 de Junio de 2026*
*Hadoop 3.3.6 | ZeroTier | Cluster 4 nodos*