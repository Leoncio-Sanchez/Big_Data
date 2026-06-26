# Fase 0 — Instalación y Configuración del Cluster

## Objetivo

Desplegar un cluster Hadoop 3.3.6 distribuido en 4 nodos interconectados via ZeroTier, con PySpark 3.5 y todas las dependencias necesarias para ejecutar el pipeline Data Lakehouse.

---

## 1. Topología del Cluster

| Nodo | Hostname | IP ZeroTier | RAM / CPU | Roles |
|:----:|----------|:-----------:|:---------:|-------|
| 1 | `leo` | 10.61.61.105 | 16GB / 16 CPUs | NameNode, ResourceManager, DataNode, NodeManager |
| 2 | `XUBUNTU` | 10.61.61.12 | 4GB / 3 CPUs | DataNode, NodeManager |
| 3 | `DEBIAN.myguest.virtualbox.org` | 10.61.61.65 | 4GB / 3 CPUs | DataNode, NodeManager |
| 4 | `isait-VirtualBox` | 10.61.61.7 | 4GB / 3 CPUs | DataNode, NodeManager |

**Usuario Hadoop:** `hadoop` (en todos los nodos, para servicios)
**Usuario operador:** `leo` (en master)

---

## 2. Red Privada ZeroTier

ZeroTier crea una VPN entre los 4 nodos para que se comuniquen como si estuvieran en la misma red local.

### Paso 1: Instalar ZeroTier en cada nodo

```bash
curl -s https://install.zerotier.com | sudo bash
```

### Paso 2: Unir cada nodo a la red

```bash
sudo zerotier-cli join <NETWORK_ID>
```

### Paso 3: Autorizar nodos desde ZeroTier Central

1. Ir a https://my.zerotier.com
2. Seleccionar la red
3. Marcar los 4 miembros como autorizados
4. Asignar IPs estáticas: 10.61.61.105 (leo), 10.61.61.12 (XUBUNTU), 10.61.61.65 (DEBIAN), 10.61.61.7 (isait-VirtualBox)

### Paso 4: Verificar conectividad entre nodos

```bash
# Desde el master (leo), probar ping a cada worker
ping -c 3 10.61.61.12   # XUBUNTU
ping -c 3 10.61.61.65   # DEBIAN
ping -c 3 10.61.61.7    # isait-VirtualBox

# Ver la red
sudo zerotier-cli listnetworks
```

### Paso 5: Agregar hosts en /etc/hosts (todos los nodos)

Para poder usar nombres en vez de IPs:

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

## 3. Prerrequisitos en Cada Nodo

### Java 11 (obligatorio para Hadoop 3.x)

```bash
sudo apt update && sudo apt install -y openjdk-11-jdk

# Verificar
java -version
# openjdk version "11.0.x"
```

### SSH (comunicación entre nodos)

```bash
sudo apt install -y openssh-server openssh-client
```

### Otras herramientas

```bash
sudo apt install -y curl wget rsync sshpass python3 python3-pip
```

---

## 4. Hadoop 3.3.6

### Paso 1: Descargar e instalar en el master (leo)

```bash
cd /tmp
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar -xzf hadoop-3.3.6.tar.gz -C /usr/local
sudo mv /usr/local/hadoop-3.3.6 /usr/local/hadoop
sudo chown -R leo:leo /usr/local/hadoop
```

### Paso 2: Variables de entorno en ~/.bashrc (master)

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

Aplicar: `source ~/.bashrc`

### Paso 3: SSH sin contraseña (key-based)

```bash
# Generar clave en el master
ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa

# Acceso a sí mismo
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

### Paso 4: Archivos de Configuración

**`$HADOOP_HOME/etc/hadoop/hadoop-env.sh`**

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

**`core-site.xml`** — Define el NameNode (IP ZeroTier de leo)

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

**`hdfs-site.xml`** — Configura replicación, bloques, WebHDFS

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

**`yarn-site.xml`** — ResourceManager en leo, 3GB asignados para contenedores por nodo peón (4GB físico, 3 CPUs)

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
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>3072</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>3</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>3072</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>512</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>
```

**`mapred-site.xml`**

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

**`workers`** (antes `slaves` en Hadoop 2)

```
XUBUNTU
DEBIAN
isait-VirtualBox
```

### Paso 5: Distribuir Hadoop a los workers

```bash
cd /usr/local
tar czf hadoop.tar.gz hadoop/

for node in XUBUNTU DEBIAN isait-VirtualBox; do
    scp hadoop.tar.gz leo@$node:/tmp/
    ssh leo@$node "sudo tar -xzf /tmp/hadoop.tar.gz -C /usr/local && sudo chown -R leo:leo /usr/local/hadoop"
done

rm hadoop.tar.gz
```

### Paso 6: Variables de entorno en workers (~/.bashrc)

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

export HDFS_NAMENODE_USER=leo
export HDFS_DATANODE_USER=leo
export YARN_NODEMANAGER_USER=leo
```

### Paso 7: Arrancar el cluster

```bash
# Solo primera vez: formatear NameNode
hdfs namenode -format

# Iniciar HDFS (NameNode + SecondaryNameNode + DataNodes)
$HADOOP_HOME/sbin/start-dfs.sh

# Iniciar YARN (ResourceManager + NodeManagers)
$HADOOP_HOME/sbin/start-yarn.sh

# Alternativa: ambos a la vez
$HADOOP_HOME/sbin/start-all.sh
```

### Paso 8: Verificar instalación

```bash
# Procesos Java en master
jps
# Debería ver: NameNode, SecondaryNameNode, ResourceManager, DataNode, NodeManager

# Procesos en workers (via SSH)
ssh XUBUNTU "jps"
# Debería ver: DataNode, NodeManager

# HDFS
hdfs dfsadmin -report
# 4 DataNodes, capacidad ~509 GB

# YARN
yarn node -list
# 4 nodos RUNNING

# Prueba读写
hdfs dfs -mkdir /test
echo "Hola cluster" | hdfs dfs -put - /test/saludo.txt
hdfs dfs -cat /test/saludo.txt
```

---

## 5. PySpark 3.5

```bash
# Instalar via pip (incluye Spark completo)
pip install pyspark

# Verificar
python3 -c "import pyspark; print(pyspark.__version__)"
# 3.5.0
```

Spark no requiere instalación separada de binarios — PySpark incluye todo.

---

## 6. Variables de Entorno Adicionales

Se agregan a `~/.bashrc` del master para persistencia:

```bash
# Hadoop & Spark - ZeroTier cluster
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_LOCAL_IP=10.61.61.105
```

**`HADOOP_CONF_DIR` y `YARN_CONF_DIR`:** Necesarias para que `spark-submit` encuentre la configuración de YARN.
**`SPARK_LOCAL_IP`:** Fuerza a Spark a usar la IP ZeroTier (10.61.61.105) en vez de la WiFi (10.70.84.39).

---

## 7. Dependencias Python

```bash
pip install --break-system-packages hdfs      # WebHDFS para Python puro
pip install --break-system-packages requests   # Descarga HTTP
pip install --break-system-packages streamlit  # Dashboard web
pip install --break-system-packages plotly     # Gráficos interactivos
pip install --break-system-packages pandas     # DataFrames
```

> `--break-system-packages` es necesario en Debian 13+ (PEP 668).

---

## 8. Puertos Clave del Cluster

| Puerto | Servicio | Nodo | Uso |
|:------:|----------|:----:|-----|
| 9000 | NameNode RPC | leo | Clientes HDFS (Spark, CLI) |
| 9870 | NameNode Web UI | leo | Interfaz web HDFS |
| 8088 | ResourceManager Web UI | leo | Interfaz web YARN |
| 9864 | DataNode HTTP | todos | Info de DataNode |
| 8042 | NodeManager Web UI | todos | Info de NodeManager |
| 9866 | DataNode transfer | todos | Transferencia de datos |
| 8032 | ResourceManager RPC | leo | Envío de aplicaciones |
| 13562 | MapReduce Shuffle | todos | Shuffle |

---

## 9. Resumen de lo Instalado

| Componente | Versión | Método |
|-----------|:-------:|--------|
| Hadoop | 3.3.6 | Tarball en /usr/local/hadoop |
| Spark | 3.5.0 | pip install pyspark |
| Java | OpenJDK 11 | apt |
| ZeroTier | latest | Script oficial |
| Python | 3.13 | apt |
| hdfs | latest | pip |
| streamlit | latest | pip |
| plotly | latest | pip |

---

## 10. Web UIs

| Interfaz | URL |
|----------|-----|
| NameNode | http://10.61.61.105:9870 |
| ResourceManager | http://10.61.61.105:8088 |
| DataNode (leo) | http://10.61.61.105:9864 |
| NodeManager (leo) | http://10.61.61.105:8042 |
