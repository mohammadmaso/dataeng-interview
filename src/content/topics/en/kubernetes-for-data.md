---
order: 137
tid: "kubernetes-for-data"
track: "13"
section: null
num: "13.2"
title: "Kubernetes for Data Workloads"
type: "technology"
level: "advanced"
priority: "P1"
est_hours: 5
summary: "StatefulSets and persistent volumes for stateful systems, operators (Strimzi for Kafka, Spark Operator, Flink Operator, ClickHouse Operator), resource management (requests/limits), and challenges of running databases on Kubernetes."
covers: ["local PV vs Ceph RBD for Kafka/ClickHouse","pod anti-affinity","YuniKorn/Volcano for batch scheduling"]
interview_qs: []
extras: []
prerequisites: ["docker"]
related: ["spark-on-kubernetes", "yarn-vs-k8s", "kafka-architecture", "clickhouse", "dataops-cicd", "onprem-iran"]
---
## 🎯 TL;DR

> Kubernetes runs containers across many machines and keeps them in the **desired state** you declare. For **stateless** services that is easy. For **stateful** data systems (Kafka, ClickHouse, Postgres, HDFS-style storage) the hard parts are **storage that follows the pod**, **ordered identity**, **resource isolation** and **safe upgrades**.

- A **Pod** is the smallest unit: one or more containers sharing a network and volumes. Pods are disposable; their IPs change.
- A **StatefulSet** gives pods stable names (`kafka-0`, `kafka-1`) and stable storage through **PersistentVolumeClaims**.
- **Requests** tell the scheduler what a pod needs; **limits** cap what it may use. Get both right or jobs starve or get killed.
- An **Operator** is a controller that encodes the operations of a specific system (rolling restarts, rebalancing, scaling). Strimzi, Spark Operator, Flink Kubernetes Operator and the ClickHouse operators are examples.
- Running a database on Kubernetes is possible, but the storage layer and the operator decide whether it is safe. Do not assume it.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A hotel manager. You tell the manager "I need three rooms with a view, always". If a room breaks, the manager moves you. For a guest who needs the same room every night (a stateful service), the manager must keep your room number and your luggage (the data) with you, not just hand you any free room.

Kubernetes objects you will see in data platforms:

| Object | What it is | Data example |
|---|---|---|
| Pod | One or more containers that run together | One Spark executor or one ClickHouse server |
| Deployment | Keeps N identical, stateless pods running | A REST API that serves a dashboard |
| StatefulSet | Pods with stable names and stable storage | Kafka brokers, ZooKeeper-style or KRaft controllers |
| Service | A stable network name for a set of pods | `clickhouse.analytics.svc` |
| PersistentVolumeClaim (PVC) | A request for storage that outlives a pod | 500 GB for one Kafka broker |
| ConfigMap / Secret | Configuration and sensitive values | Connection settings, credentials |

A minimal Deployment for a stateless service:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schema-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: schema-api
  template:
    metadata:
      labels:
        app: schema-api
    spec:
      containers:
        - name: api
          image: registry.internal/schema-api:1.4.2
          ports:
            - containerPort: 8080
          resources:
            requests:
              cpu: "250m"
              memory: "256Mi"
            limits:
              memory: "512Mi"
```

If a pod dies, the Deployment creates a new one with a new name and a new IP. That is fine for an API. It is not fine for a database that keeps files on local disk.

## 🔧 How it really works (Mid)

### Desired state and controllers

You write the desired state (`replicas: 3`). A controller compares it with the actual state and acts: it creates, deletes or replaces pods until they match. Nothing runs "once" unless you ask for a Job. This is why a crashed pod comes back, and why an error in the spec can silently keep trying forever.

### StatefulSet: identity and storage

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: clickhouse
spec:
  serviceName: clickhouse-headless
  replicas: 3
  selector:
    matchLabels:
      app: clickhouse
  template:
    metadata:
      labels:
        app: clickhouse
    spec:
      containers:
        - name: clickhouse
          image: clickhouse/clickhouse-server:24.8   # pin the version you tested
          resources:
            requests:
              cpu: "4"
              memory: "16Gi"
            limits:
              memory: "16Gi"
          volumeMounts:
            - name: data
              mountPath: /var/lib/clickhouse
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: local-fast
        resources:
          requests:
            storage: 2Ti
```

What this gives you:

- Pods are named `clickhouse-0`, `clickhouse-1`, `clickhouse-2`, created in order.
- Each pod gets its **own** PVC, named `data-clickhouse-0`, and the claim stays with that ordinal when the pod is rescheduled.
- A headless Service (`clickhouse-headless`) gives each pod a stable DNS name, such as `clickhouse-0.clickhouse-headless`, which replication configs can use.

Deleting a StatefulSet does **not** delete its PVCs by default. That protects data, and it also means old disks keep costing money until you remove them.

### Storage: why the volume type decides everything

A PersistentVolume (PV) is the actual storage. The `storageClassName` chooses how it is provisioned.

| Storage type | Pros | Cons for data systems |
|---|---|---|
| Local disks (local PV) | Lowest latency, high throughput, cheap | Pod is pinned to the node; node loss means data must be replicated by the application |
| Network block storage (for example Ceph RBD) | Pod can move to another node and reattach | Extra latency, more shared failure domains, performance tuning needed |
| Object storage via CSI or an S3 gateway | Huge capacity, cheap | Not a POSIX filesystem; most databases should not use it as their data directory |

For a Kafka broker or a ClickHouse replica, the trade-off is real. Local PVs give the fastest disk but tie the pod to a node, so **replication at the application level** (Kafka replication factor 3, ClickHouse replicas through Keeper) must survive a node loss. Network storage makes rescheduling easy but adds latency and a shared storage failure domain. Decide per system, and test the failure you actually expect.

### Scheduling: requests, limits and placement

- **Requests** are what the scheduler reserves. A pod is placed only on a node with enough free requested CPU and memory.
- **Limits** are the cap. CPU over the limit is throttled; memory over the limit gets the container **OOMKilled**.
- Set memory **request equal to limit** for databases and JVM services, so the kernel does not kill them under node pressure. Leave a margin for the JVM's off-heap memory.
- Use **pod anti-affinity** so replicas of the same system do not land on the same node:

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app: clickhouse
        topologyKey: kubernetes.io/hostname
```

For zone-level spreading, use `topology.kubernetes.io/zone` as the `topologyKey`.

### Operators

A plain StatefulSet does not know how to add a Kafka broker, rebalance partitions or restart a ClickHouse replica in the right order. An **Operator** watches a custom resource (for example a `Kafka` or `ClickHouseInstallation` object) and runs the system's own procedure:

- **Strimzi** manages Kafka clusters, topics and users.
- **Spark Operator** (Kubeflow implementation) submits and tracks Spark applications from a `SparkApplication` resource.
- **Flink Kubernetes Operator** manages Flink deployments and upgrades with savepoints.
- **ClickHouse operators** (community and vendor-maintained versions) manage cluster topology and configuration.

Check each operator's documentation for the API version and the supported features of your release; these change often.

### Batch scheduling

The default scheduler places pods one by one. A Spark job needs 50 executors at once or none, otherwise it may hold half the cluster and wait forever (a deadlock). Gang scheduling systems such as **Volcano** and **YuniKorn** place a whole group together, and queues give each team a share. This is the Kubernetes answer to what YARN queues did; see [[yarn-vs-k8s]].

## 🧠 Senior depth & trade-offs

- **Stateful workloads are an application decision.** Kubernetes provides stable identity and volumes. Replication, leader election, rebalancing and backup are still the system's job. A StatefulSet with replication factor 1 is a single point of failure with extra steps.
- **Local vs network storage for Kafka.** Kafka replicates at the application level, so local disks with replication factor 3 across zones usually give the best throughput per dollar, and a broker loss is a rebalance, not an outage. Network storage duplicates replication and adds latency to every fsync. The choice depends on the disk budget and the failure model.
- **ClickHouse and merges.** Background merges are I/O heavy. Give pods dedicated nodes or node pools, and keep CPU requests high enough that merges do not starve queries. Watch the number of parts per partition in monitoring.
- **Upgrades are the real risk.** StatefulSets roll pods one at a time in ordinal order by default, but "one at a time" is not enough for a quorum system. Operators understand this; raw YAML does not. Test a rolling restart with a load running, and measure lag and query latency during it.
- **Resource isolation between jobs.** Without limits, a rogue Spark job evicts a Kafka broker. Use namespaces with ResourceQuotas, LimitRanges, PriorityClasses, and separate node pools for stateful and batch workloads.
- **Observability.** Track pod restarts, OOMKilled events, PVC usage, disk latency, throttled CPU, and the operator's reconcile errors. A pod that restarts quietly every hour is a data loss risk.
- **Backups.** A PVC snapshot is not a backup until you test restore. Use the system's own backup tool (for example, a consistent snapshot per replica, or a managed export) and verify restores on a schedule.
- **When not to run it on Kubernetes.** A small, stable, single-node database with a dedicated VM and a good backup plan can be simpler. Run it on Kubernetes when you need the scheduling, self-healing and standard tooling, and you have someone who understands the storage layer.

> **⚠️ Watch out:** Deleting a StatefulSet, or the PVC behind it, can delete the only copy of a replica. Check the `persistentVolumeReclaimPolicy` of the storage class before you delete anything, and never run `kubectl delete pvc` on a database without a verified backup.

## 🧪 Try it

You need a local cluster (for example kind or minikube) and `kubectl`.

1. Apply the Deployment from the Junior section. Scale it to 5, delete one pod, and explain what changed in the pod names and IPs. Then explain why the same behaviour is a problem for a database with a local disk.
2. Write a StatefulSet with 3 replicas and a 1 GiB volume claim. Delete `pod-1` and explain which PVC is reattached. Then explain what would happen to the data if you deleted the StatefulSet with `--cascade=orphan` and then re-created it with the same name.
3. Add pod anti-affinity to the StatefulSet and explain what it prevents when the cluster has one node versus three nodes.

<details><summary>Show solutions</summary>

**1.** After scaling to 5, there are five pods with random suffixes. Deleting one creates a replacement with a new name and a new IP. Nothing in the app should depend on the IP. For a database with a local disk, the replacement pod has no data, so it must rejoin and resynchronize, or the data on the old node is stranded.

**2.** `pod-1` gets its own PVC `data-<name>-1` again, because the claim name is derived from the StatefulSet and ordinal. With orphan deletion, the pods are removed but the PVCs remain, so re-creating the StatefulSet with the same name reattaches the existing claims and the data. This is the mechanism behind "safe" recreation, but only when the names and sizes match.

**3.** On a one-node cluster, required anti-affinity makes the extra replicas unschedulable (they stay `Pending`), which is correct: they would share a failure domain. On three nodes, the scheduler spreads one replica per node. In production, a single node loss then costs one replica, not all of them.

</details>

## 💬 Interview corner

**Question: "Would you run Kafka on Kubernetes?"**

- **Mid answer:** "Yes, with Strimzi, using StatefulSets and persistent volumes, with pod anti-affinity so brokers spread across nodes."
- **Senior answer:** "I'd use Strimzi for the lifecycle, but the storage decision comes first. Local PVs give throughput and need replication factor 3 across failure domains, so a node loss is a rebalance. Network storage makes rescheduling easy but adds latency and a shared failure domain. I'd also set requests equal to limits, use dedicated node pools, and test a rolling restart under load before going to production."

**Question: "What is the difference between a Deployment and a StatefulSet?"** Deployments give interchangeable pods with random names and no stable storage. StatefulSets give ordered, stable names and per-pod persistent volumes that follow the pod's identity.

> **🎤 Say this in the interview:** "Kubernetes gives me desired state, identity and storage primitives. Whether a data system is safe on it depends on replication, the operator, and the storage class. I choose local or network storage based on the failure model, and I always test the restore path."

## ⚠️ Common mistakes

> **⚠️ Watch out:** Setting only memory limits and no requests. The scheduler then packs pods onto nodes that cannot sustain them, and they are OOMKilled under load.

- Running a single replica of a stateful system and calling it highly available.
- Forgetting that a PVC outlives its pod and its StatefulSet, so old storage keeps costing money.
- Using a Deployment for a database, then losing its data when a pod is rescheduled.
- Omitting pod anti-affinity, so all replicas land on one node.
- Setting CPU limits too low for merges or compaction, which causes throttling and lag.
- Letting batch jobs and brokers share nodes without quotas or priority classes.
- Upgrading an operator or a database image without testing the rolling procedure.

## ✅ Check yourself (quiz)

1. Which Kubernetes object gives each pod a stable name and its own persistent volume?
   - A) Deployment
   - B) Service
   - C) StatefulSet
   - D) ConfigMap

<details><summary>Answer</summary>

**C** — StatefulSets provide ordered stable names and per-pod PVCs that follow the pod's identity. A Deployment gives interchangeable pods with random names.

</details>

2. A Spark executor pod is OOMKilled repeatedly. Its memory limit is set, but its request is much lower. What is the most likely issue?
   - A) The request should be zero
   - B) The container exceeds its memory limit; check the executor memory settings and the off-heap overhead against the limit
   - C) CPU requests cause OOM kills
   - D) Kubernetes does not support memory limits for Spark

<details><summary>Answer</summary>

**B** — OOMKilled means the container used more memory than its limit. Compare the JVM heap and off-heap overhead with the limit, and size them together.

</details>

3. Why does pod anti-affinity matter for a three-replica database?
   - A) It makes the pods start faster
   - B) It keeps replicas on different nodes, so a single node failure does not remove all copies
   - C) It removes the need for replication
   - D) It increases the storage size

<details><summary>Answer</summary>

**B** — placement is a failure-domain decision. Replication only helps if the replicas do not share the same failure.

</details>

4. **(Senior)** You must choose storage for a ClickHouse cluster. Local NVMe PVs or network block storage? Give the decision criterion.

   - A) Always network storage, because it is more modern
   - B) Local NVMe gives the best query and merge throughput and the application replicates data; network storage eases rescheduling but adds latency and a shared failure domain. Choose based on the failure model and whether the replication and recovery procedure are tested
   - C) Always local storage, because network storage cannot be replicated
   - D) The choice does not matter for analytics

<details><summary>Answer</summary>

**B** — local storage is fast but pins data to nodes, so replication must survive node loss. Network storage trades latency and a shared failure domain for mobility. The right answer depends on the replication design and the tested recovery path.

</details>

5. **(Senior)** A team proposes deleting and re-creating a StatefulSet to "reset" a broken Kafka cluster. What do you check first?

   - A) Nothing; re-creating is always safe
   - B) Whether the PVCs and the reclaim policy will keep or delete the data, whether the operator and the cluster ID will reconcile, and whether a verified backup exists before any destructive step
   - C) Whether the topic names are in uppercase
   - D) Whether the cluster has more than 100 nodes

<details><summary>Answer</summary>

**B** — re-creating a stateful workload can reattach data, delete it, or create a second cluster with a conflicting identity. Verify storage and reclaim behaviour, metadata, and backups before acting.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Pod | The smallest deployable unit: one or more containers sharing network and volumes; disposable |
| Deployment vs StatefulSet | Deployment: interchangeable pods, random names; StatefulSet: stable ordered names and per-pod storage |
| PersistentVolumeClaim | A request for storage that can outlive the pod that uses it |
| Request vs limit | Request: reserved for scheduling; limit: the cap; memory over limit causes OOMKilled |
| Why memory request = limit for JVM databases | Avoids eviction under node pressure and makes capacity predictable |
| Pod anti-affinity | A placement rule that keeps replicas on different nodes or zones |
| Operator | A controller that encodes a specific system's operational procedures through a custom resource |
| Local PV vs network storage | Local: fastest, pinned to a node, needs app-level replication; network: mobile, extra latency, shared failure domain |

## 🔗 Go further

- **[[docker]]** covers the container images that these pods run.
- **[[spark-on-kubernetes]]** shows Spark executors and drivers as pods, with the operators and resource settings they need.
- **[[yarn-vs-k8s]]** compares Kubernetes scheduling with YARN queues and data locality.
- **[[kafka-architecture]]** explains the replication and failure model that determines your storage choice for brokers.
- **[[clickhouse]]** covers replicas, merges and TTL, which matter for the StatefulSet sizing above.
- **[[onprem-iran]]** discusses running this kind of cluster on your own hardware, where storage and node failure planning are the main constraints.
- Read the Kubernetes docs for StatefulSets, storage classes and scheduling; then read the operator documentation for the exact resource versions your cluster uses.

