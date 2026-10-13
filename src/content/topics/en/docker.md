---
order: 136
tid: "docker"
track: "13"
section: null
num: "13.1"
title: "Containers & Docker"
type: "technology"
level: "beginner"
priority: "P1"
est_hours: 2
summary: "Images, containers, layers, Docker Compose for local pipeline dev environments (Kafka + Postgres + Spark)."
covers: []
interview_qs: []
extras: []
prerequisites: ["linux-shell"]
related: ["kubernetes-for-data", "dataops-cicd", "spark-on-kubernetes", "postgresql"]
---
## 🎯 TL;DR

> A **container** is an isolated process that runs from an **image**: a read-only stack of **layers** that holds your code, its runtime and its libraries. **Docker** builds images, runs containers, and with **Docker Compose** starts several containers together, for example Kafka, Postgres and your Spark job on one laptop.

- An **image** is a template; a **container** is a running instance of it.
- Each Dockerfile instruction creates a **layer**. Layers are cached and shared, so order matters for build speed.
- A container gets its own filesystem view, network and process tree, but **shares the host kernel**. It is not a virtual machine.
- **Volumes** keep data outside a container's writable layer. Without them, data disappears with the container.
- Use Compose for local development and tests. Production orchestration is a different tool; see [[kubernetes-for-data]].

## 🌱 Start simple (Junior)

> **💡 Analogy:** A shipping container. The box is the same whether it carries bananas or electronics, and it can move from a ship to a truck without repacking. An image is the sealed box with everything inside; a container is the box being used on a particular truck.

Why data engineers care: a Spark job works on your laptop, fails on the cluster, because the Python version differs. A container packages the exact Python, Java and libraries, so "works on my machine" becomes "works in the image".

Three commands cover most of the basics:

```bash
docker pull postgres:16            # download an image from a registry
docker run --rm -it postgres:16 psql --version   # run a container from it, then remove it
docker ps                          # list running containers
```

The `:16` part is a **tag**. Tags are labels that can move; for reproducible builds, pin a specific version or a digest (`image@sha256:...`).

## 🔧 How it really works (Mid)

### Images, layers and the build cache

A Dockerfile describes how to build an image:

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# 1) Dependencies first: this layer changes rarely, so it stays cached.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Code last: this layer changes on every commit.
COPY pipeline/ ./pipeline/

# Do not run as root.
RUN useradd --create-home appuser
USER appuser

ENTRYPOINT ["python", "-m", "pipeline.load"]
```

Each instruction that changes the filesystem (`COPY`, `RUN`, `ADD`) creates a layer. When you change `pipeline/`, Docker reuses the cached layers up to the `COPY requirements.txt` and reinstalls nothing. If you had copied the whole project before `pip install`, every code change would reinstall all dependencies.

Layers are content-addressed and shared. Ten images based on `python:3.12-slim` store that base layer once on disk.

### Containers, processes and namespaces

When you run a container, Linux gives it:

- its own **process namespace** (PID 1 inside the container is your entrypoint),
- its own **network namespace** (an IP address and ports on a virtual network),
- its own **mount namespace** (the image filesystem plus a thin writable layer on top),
- resource limits through **cgroups** (CPU and memory).

The container uses the host kernel directly. This is why containers start in milliseconds, and why a container cannot run a different OS kernel. A Linux container on a Windows host runs through a Linux VM in the background.

### Storage: writable layer, volumes and bind mounts

| Storage | Where the data lives | Survives `docker rm`? | Use for |
|---|---|---|---|
| Writable layer | Inside the container | No | Temporary files only |
| Named volume | Managed by Docker | Yes | Database files (Postgres data dir) |
| Bind mount | A host path you choose | Yes (it is on the host) | Source code during development |

Writing a lot of data into the writable layer is slow and makes containers heavy. Put databases and output on volumes.

### Networking inside Compose

Compose creates a private network for the project. Containers reach each other by **service name**: a job connects to `postgres:5432`, not to `localhost`. Only ports you publish with `ports:` are reachable from the host.

### A realistic local stack

```yaml
# compose.yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: dev
      POSTGRES_PASSWORD: dev_only_password
      POSTGRES_DB: warehouse
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dev -d warehouse"]
      interval: 5s
      timeout: 3s
      retries: 10

  loader:
    build: .
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgresql://dev:dev_only_password@postgres:5432/warehouse

volumes:
  pgdata:
```

Run it with `docker compose up --build`. The loader starts only after Postgres reports healthy. Without the healthcheck, `depends_on` only waits for the container to **start**, not for the database to accept connections, and the loader would crash on its first try.

Secrets like `dev_only_password` are fine for a local stack, and must never be the production value. Use an env file that is not committed, or a secret store.

## 🧠 Senior depth & trade-offs

- **Image size and attack surface.** Use multi-stage builds: compile in a builder image with build tools, copy only the artifacts into a slim runtime image. Smaller images pull faster in a cluster and carry fewer vulnerable packages.
- **Pin and scan.** Pin base images by digest in production, rebuild regularly for security patches, and scan images in CI. "It worked last month" is not reproducibility if the tag moved.
- **Least privilege at runtime.** Run as a non-root user, drop capabilities you do not need, and mount secrets rather than baking them into layers. Anything written in a `RUN` step stays in the image history even if a later step deletes it.
- **Data gravity in containers.** Stateful systems (databases, Kafka brokers) in containers need durable volumes and a plan for node loss. For a laptop, a named volume is enough. For a cluster, you need the storage and scheduling guarantees covered in [[kubernetes-for-data]].
- **Java and memory.** JVM processes must respect container memory limits. Recent JVMs read cgroup limits, but you should still set heap size explicitly and leave room for off-heap memory. Check the documentation of your JVM version.
- **Build reproducibility.** Non-pinned `apt-get install` or `pip install` without versions gives different images on different days. Pin dependency versions with a lock file.
- **Local vs production.** Compose is excellent for integration tests with real Kafka, Postgres and Spark. It is not a scheduler, it has no multi-node failover, and it is not where production state should live.
- **When not to containerize.** A tiny script run once a month may not need an image. A GPU-bound or hardware-specific workload may be easier on bare metal, depending on your drivers and cluster.

> **⚠️ Watch out:** `localhost` inside a container is the container itself. A job in one container connecting to `localhost:5432` will fail even when Postgres runs on your host. Use the service name in Compose, or `host.docker.internal` on Docker Desktop to reach the host.

## 🧪 Try it

You need Docker with the Compose plugin installed.

1. Write a Dockerfile for a small Python script that prints the current row count of a table in Postgres. Use the Compose file above, mount the script as a bind mount, and run it once. Then reorder the Dockerfile so that `pip install` runs before the code is copied, and explain what changes in the build cache when you edit the script.
2. Run `docker compose up -d postgres`, create a table, then run `docker compose down` and `docker compose up -d postgres` again. Explain whether the table still exists, and why. Then run `docker compose down -v` and check again.
3. Find the image size of `python:3.12` and `python:3.12-slim` with `docker images`. Explain what the difference contains and why it matters for a production image.

<details><summary>Show solutions</summary>

**1.** Editing `main.py` changes only the last `COPY` layer; the `pip install` layer is reused from cache, so the rebuild takes seconds. In the original order, copying the whole project before `pip install` invalidates the install layer on every edit.

**2.** The table still exists after `down` and `up` because the named volume `pgdata` is preserved. `down -v` removes named volumes too, so the table is gone. The lesson: `down` keeps data, `down -v` destroys it.

**3.** The full image includes compilers, headers and many system packages used for building Python libraries. The slim image contains only the runtime. Smaller images pull faster and expose fewer packages to vulnerability scanners. Sizes change between releases, so read the values from your own `docker images` output.

</details>

## 💬 Interview corner

**Question: "What is the difference between an image and a container?"**

- **Mid answer:** "An image is a read-only template built from layers. A container is a running instance of an image with its own writable layer, network and process."
- **Senior answer:** "An image is immutable and content-addressed, so the same digest always means the same filesystem. A container adds a thin writable layer, namespaces and cgroup limits on the host kernel. For data, the important part is that the writable layer is disposable, so state belongs in volumes or external services."

**Question: "Why would you use containers for a data pipeline?"**

Answer with reproducibility (pinned runtime and dependencies), isolation (conflicting library versions), and portability (the same image runs in CI, staging and the cluster). Mention that the image is not the whole story: configuration, secrets and state must be handled separately.

> **🎤 Say this in the interview:** "I containerize a job so its runtime and dependencies are versioned with the code. Images are built in CI with pinned bases, run as non-root, and get config and secrets from outside the image. State lives in volumes or managed services, never in the container layer."

## ⚠️ Common mistakes

> **⚠️ Watch out:** `docker compose down -v` deletes named volumes, which means the database is gone. Make it a conscious command, not a habit.

- Copying the whole source tree before installing dependencies, so every code change rebuilds everything.
- Using `:latest` in production, so two deployments of the "same" image differ.
- Writing database files into the container's writable layer instead of a volume.
- Putting passwords in a Dockerfile `ENV` line or a committed Compose file; they stay in the image history.
- Running everything as root.
- Assuming `depends_on` waits for a database to be ready; it waits only for the container to start unless you add a healthcheck.
- Treating a container as a VM and SSH-ing into it to fix things. Rebuild the image instead.

## ✅ Check yourself (quiz)

1. What is the relationship between an image and a container?
   - A) They are the same thing with different names
   - B) An image is a read-only template; a container is a running instance of it
   - C) A container is a template; an image is a running process
   - D) An image always contains a full operating system kernel

<details><summary>Answer</summary>

**B** — images are immutable templates made of layers; containers are running instances with a writable layer added on top.

</details>

2. You edit one line of application code and rebuild. The `pip install` step runs again and takes 3 minutes. What is the most likely Dockerfile problem?
   - A) The base image is too big
   - B) `COPY . .` (or the code copy) comes before `pip install`, so the cache for the dependency layer is invalidated on every code change
   - C) `pip` cannot be cached
   - D) The container needs more memory

<details><summary>Answer</summary>

**B** — copy the dependency file first, install, then copy the code. Layers after a changed layer are rebuilt, so the order of instructions controls the cache.

</details>

3. A pipeline container cannot reach Postgres at `localhost:5432`. Postgres runs in another container of the same Compose project. What should the connection string use?
   - A) `localhost:5432`
   - B) The Compose service name, for example `postgres:5432`
   - C) The container's random ID
   - D) `127.0.0.1` with a different port

<details><summary>Answer</summary>

**B** — inside a container, `localhost` refers to that container. Compose gives services DNS names matching their service names on the project network.

</details>

4. Which storage choice keeps a Postgres database after `docker compose down`?
   - A) The writable layer of the container
   - B) A named volume
   - C) An environment variable
   - D) A `RUN` step in the Dockerfile

<details><summary>Answer</summary>

**B** — named volumes survive container removal. `down -v` is the exception that removes them.

</details>

5. **(Senior)** Your team's production image is built from `python:3` and the build is not reproducible. Propose a concrete change set.

   - A) Use `:latest` so you always get the newest patches
   - B) Pin the base image to a specific version or digest, pin dependencies with a lock file, use a multi-stage build, run as non-root, and rebuild and scan in CI on a schedule
   - C) Commit the virtual environment into the image
   - D) Switch to running the job on a laptop

<details><summary>Answer</summary>

**B** — reproducibility needs pinned inputs. Rebuilding on a schedule still gets security patches, but deliberately, and the digest or version change is visible in code review.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Image vs container | Image: immutable layered template; container: running instance with a writable layer |
| Why order Dockerfile instructions carefully | Changed layers invalidate all later layers in the build cache |
| Container vs VM | A container shares the host kernel and isolates via namespaces and cgroups; a VM runs its own kernel |
| `localhost` inside a container | Refers to the container itself, not the host or other containers |
| Named volume vs writable layer | Volumes survive container removal; the writable layer is lost with the container |
| `depends_on` alone | Waits for the container to start, not for the service to be ready; add a healthcheck with `condition: service_healthy` |
| Why pin image tags or digests | A tag like `latest` can move, so the same name can mean different images over time |
| `docker compose down -v` | Stops and removes containers and named volumes, so the data is deleted |

## 🔗 Go further

- **[[kubernetes-for-data]]** covers running stateful data systems in a cluster, where containers need durable storage and scheduling.
- **[[dataops-cicd]]** shows how images are built, tested and deployed in CI/CD.
- **[[linux-shell]]** and **[[os-jvm-essentials]]** explain the processes, namespaces and memory behaviour underneath containers.
- **[[spark-on-kubernetes]]** shows Spark jobs running as containers in a cluster.
- **[[postgresql]]** covers the database used in the Compose example.
- Read Docker's official documentation on Dockerfile best practices and Compose healthchecks; syntax and defaults change over time.

