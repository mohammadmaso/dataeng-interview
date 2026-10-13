---
order: 37
tid: "jvm-languages-for-de"
track: "3"
section: null
num: "3.8"
title: "Java/Scala for Data Systems"
type: "technology"
level: "intermediate"
priority: "P2"
est_hours: 3
summary: "Spark, Flink and Kafka are JVM-native. Knowing Java or Scala well enough to read source, write UDFs or connectors, and read stack traces is an advantage."
covers: ["Scala basics for Spark","Flink DataStream API in Java","Kafka client in Java","when to choose JVM over Python"]
interview_qs: []
extras: []
prerequisites: ["os-jvm-essentials"]
related: ["pyspark-api", "spark-architecture", "spark-catalyst", "flink", "kafka-streams", "kafka-producers", "python-dataframes"]
---

## 🎯 TL;DR

> Much of the data engineering stack runs on the **JVM** (Java Virtual Machine): Spark, Flink, Kafka and most of their connectors. You do not need to be a Java expert, but reading their source, writing a **UDF** (user-defined function), a **connector** or a **producer**, and reading a **stack trace** without fear is a real advantage.

- **Scala** is Spark's native language. Its typed `Dataset` API is powerful, but the **DataFrame API** with column expressions is what most jobs use.
- **Java** is the language of the Flink DataStream API and the Kafka clients. Its lambdas are good enough for pipelines.
- A **Python UDF** crosses a process boundary for each batch of rows; a **JVM UDF** runs inside the executor. This is the main performance reason to choose JVM code for custom logic.
- Choose the JVM when you write engine-level code, need low-latency streaming or lean on JVM-only connectors. Stay in Python when the logic is analytical and the team's skills are in Python.

## 🌱 Start simple (Junior)

> **💡 Analogy:** The JVM is like a shared kitchen with a standard set of appliances. Spark, Flink and Kafka were all built in that kitchen, so they use the same ovens and knives. If you want to add a new dish that runs inside their kitchen (a custom UDF or a connector), it is much easier if you cook in the same language they use.

Here is the same idea, "uppercase every event", in the three places you are most likely to meet JVM code. First, the Flink job in Java:

```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UpperCaseJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> events = env.fromElements("click", "view", "click");
        DataStream<String> upper = events.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                return value.toUpperCase();
            }
        });

        upper.print();
        env.execute("upper-case");
    }
}
```

The job defines a source, one transformation and a sink (`print`), then `execute` starts it. Every Flink program follows this shape: **source → transformations → sink → execute**.

A junior-level Scala version with Spark does the same work on a table:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.upper

val spark = SparkSession.builder().appName("upper").getOrCreate()
import spark.implicits._

Seq("click", "view", "click").toDF("event")
  .withColumn("event", upper($"event"))
  .show()
```

The code is short because the logic is a column expression. The engine plans and runs it; you describe what you want.

## 🔧 How it really works (Mid)

### Why these systems are on the JVM

Spark, Flink and Kafka are server-side engines that need threads, memory management, a mature garbage collector and a large ecosystem of libraries (serialization, networking, Hadoop-compatible file systems). The JVM provides these, and the engines' core code, operators and many connectors are written in Java or Scala. Python APIs are thin layers that send work to that core (in PySpark, the Python process talks to the JVM driver and executors).

### Scala for Spark

Scala combines functional and object-oriented features and compiles to JVM bytecode. Three ideas matter for Spark work:

- **`case class`** gives an immutable record with equality and a readable `toString`. It is how you describe a typed row.
- **`Dataset[T]`** is a typed collection. The compiler checks that `o.amount` exists and is a `Double`.
- **`DataFrame`** is `Dataset[Row]`. It is untyped at compile time, but column expressions (`$"amount"`, `col("amount")`) go through the Catalyst optimizer, which can reorder, prune and push filters down.

```scala
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Order(orderId: Long, customer: String, amount: Double)

object TopCustomers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("top-customers").getOrCreate()
    import spark.implicits._

    val orders: Dataset[Order] = Seq(
      Order(1L, "ali", 10.0), Order(2L, "sara", 5.5), Order(3L, "ali", 7.0)
    ).toDS()

    // Typed API: the compiler checks fields, but the lambda is opaque to Catalyst
    orders.filter(_.amount > 6.0).map(_.customer.toUpperCase).show()

    // Column API: Catalyst sees the whole expression and can optimize it
    orders
      .groupBy($"customer")
      .agg(sum($"amount").as("total"))
      .orderBy(desc("total"))
      .show()

    spark.stop()
  }
}
```

The typed lambdas (`_.amount > 6.0`) are Scala functions that Spark cannot look inside. The column version is visible to the optimizer. That is why the rule "prefer built-in column functions over typed lambdas" is good advice for performance.

### UDFs: where JVM code earns its place

A UDF is custom logic that Spark applies to values. There are three levels, from fastest to slowest:

1. **Built-in functions** (`trim`, `lower`, `to_date`, `when`) run inside Spark's optimized engine. Always try these first.
2. **JVM UDFs** (Scala or Java) run in the executor JVM. No serialization across a process boundary. They are opaque to the optimizer, but fast.
3. **Python UDFs** run in a Python worker process next to each executor. Rows are serialized from the JVM to Python and back, which costs CPU and memory. Pandas UDFs (vectorized, Arrow-based) reduce that cost.

```scala
import org.apache.spark.sql.functions.udf

val normalize = udf((s: String) => Option(s).map(_.trim.toLowerCase).orNull)
// df.withColumn("customer_norm", normalize($"customer"))
// Better when possible: df.withColumn("customer_norm", lower(trim($"customer")))
```

Note the explicit null handling: a UDF receives `null` for a null column, and calling `s.trim` on it throws a `NullPointerException` inside an executor.

### Flink DataStream API in Java

Flink processes unbounded streams with explicit time and state. The DataStream API is the lower-level API where you control keys, windows and state. A keyed window count in Java looks like this (the pipeline shape you would describe in an interview):

```text
env.fromSource(kafkaSource, watermarks, "orders")
   .keyBy(order -> order.customerId)          // partition by key
   .window(TumblingEventTimeWindows.of(Time.minutes(5)))
   .aggregate(new SumAmount())                // per key, per 5-minute window
   .sinkTo(warehouseSink);
```

The important ideas are the same in every language: a **key** decides which parallel task owns the state, **event time** and **watermarks** decide when a window closes, and the **sink** decides delivery guarantees ([[watermark]], [[tumbling-window]], [[exactly-once]]). The Java syntax simply makes those choices explicit.

### Kafka clients in Java

The Kafka Java client is the reference implementation. Producers configure a few critical settings and then `send` records, which are sent asynchronously and return a `Future`:

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", "order-42", "{\"amount\":10.5}");
            RecordMetadata meta = producer.send(record).get(); // block only in a demo
            System.out.printf("partition=%d offset=%d%n", meta.partition(), meta.offset());
        }
    }
}
```

Three details to understand: the **key** (`"order-42"`) decides the partition, so the same key always goes to the same partition and keeps its order; `acks=all` waits for the in-sync replicas; and `enable.idempotence` prevents duplicates caused by producer retries within a session. The `try` block closes the producer, which flushes pending records. In production, avoid calling `.get()` on every send; use the callback form instead, so you do not serialize throughput.

### When to choose JVM over Python

| Situation | Lean JVM | Lean Python |
|---|---|---|
| Custom Spark or Flink operator on millions of rows per second | Yes | No |
| Writing a connector or a Kafka Connect plugin | Yes | Rarely supported |
| Reading engine source code for debugging | Yes | Mostly not needed |
| Feature engineering, notebooks, ML handoff | Possible | Yes |
| Python UDF throughput is the bottleneck | Move the logic to Scala/Java or to built-ins | Use Pandas UDFs and Arrow |
| The team already ships Python services | Adds a language to maintain | Yes |

## 🧠 Senior depth & trade-offs

- **Serialization and closures.** A Spark or Flink function that captures a non-serializable object (a database connection, a logger created in the driver) fails at runtime with `NotSerializableException`. Create such objects inside the task, once per partition, not in the closure.
- **Classloading and dependency conflicts.** Spark and Flink ship their own versions of Jackson, Guava, Netty and Hadoop libraries. A fat jar that bundles a different version can cause `NoSuchMethodError` on the cluster but not locally. Mark engine dependencies as `provided` and shade or relocate conflicting packages.
- **Version alignment.** The JDK version and the Scala binary version must match what your Spark or Flink release was built for (for example `spark-sql_2.12` versus `_2.13`). Check the release notes for your version; mismatches show up as cryptic linkage errors.
- **Garbage collection and memory.** JVM executors have heap settings that matter as much as the code. Long GC pauses look like stalled tasks. Read GC logs and the Spark UI memory tab before tuning code ([[spark-memory-tuning]]).
- **Reading stack traces.** The skill is practical: find the first frame that is in your code, then the first frame in the engine, then the "Caused by" chain at the bottom. Most failures are a `NullPointerException` from a null column, a `ClassCastException` from a schema mismatch, or a serialization error from a captured object.
- **Typed vs untyped trade-off.** Typed Datasets catch field-name typos at compile time but lose some optimizations and can be slower. For production ETL, prefer DataFrame column expressions and keep typed code at the edges.
- **Monitoring what you write.** Expose custom metrics (counters for rejected records, timers for external calls) through the engine's metrics system, and watch them next to lag and checkpoint duration ([[consumer-lag]]).
- **When not to use JVM code.** If a built-in function or a SQL expression does the job, a custom JVM UDF adds build, deployment and test cost for no gain. Write custom code only when the built-ins cannot express the logic or the performance gap is measured.

## 🧪 Try it

1. **Predict the output.** Given `Seq(Order(1L,"ali",10.0), Order(2L,"sara",5.5), Order(3L,"ali",7.0))`, what does `orders.filter(_.amount > 6.0).map(_.customer.toUpperCase).show()` print? Then write the equivalent with `$"amount" > 6.0` and `upper($"customer")` and explain which one Catalyst can optimize better.
2. **Find the bug.** A Flink `MapFunction` stores a JDBC connection in a field created in `main` and uses it inside `map`. Explain what goes wrong and how to fix it (hint: think about serialization and where the connection is opened).
3. **Change the key.** In the Kafka producer, change the record key from `"order-42"` to `null`. Explain what happens to partitioning and to ordering guarantees for one customer's events.

<details><summary>Show solutions</summary>

**1.** The filter keeps amounts above 6.0: `10.0` and `7.0`, both belonging to `ali`. The output is two rows, both `ALI`. (Sara's 5.5 is dropped.) The column version, `orders.filter($"amount" > 6.0).select(upper($"customer")).show()`, returns the same values, but the optimizer sees the predicate and projection directly and can push them down into a source that supports it. The typed lambda hides them.

**2.** The connection object is created on the driver and captured by the function. The function is serialized and shipped to each task, and a JDBC connection is not serializable, so the job fails with `NotSerializableException`. The fix is to open the connection inside the task, for example in `open(Configuration)` of a `RichMapFunction`, and close it in `close()`.

**3.** With a `null` key, the producer assigns partitions round-robin or with a sticky partitioner (depending on the client version). Events for one customer may spread across partitions, so their order is no longer guaranteed. Keying by `customer_id` puts each customer's events in one partition, which preserves order for that customer, at the cost of possible skew if one customer is very large ([[hot-key]]).

</details>

## 💬 Interview corner

**Question: "Why would a data engineer learn Scala or Java when the team uses PySpark?"**
Mid answer: "To write UDFs faster and to read the Spark source."
Senior answer: "PySpark is a thin layer over JVM code. When I need a custom operator, a connector or to diagnose a serialization or shuffle failure, the JVM stack is where the answer lives. JVM UDFs avoid the Python serialization cost. I'd still start with built-ins and DataFrame expressions, and only drop to the JVM when the measurements justify it."

**Question: "A Spark job fails with `NotSerializableException`. What do you check?"**
Mid: "Whether a non-serializable object is captured in a closure."
Senior: "I find the object in the stack trace and trace what the lambda captures. I move the object creation into the task (per partition), or use a serializable factory, and I check whether a class-level field of the enclosing object is pulling in more state than intended."

> **🎤 Say this in the interview:** "I think of PySpark as the front door and the JVM as the building. I write in Python by default, and I go into Scala or Java when I need engine-level code, a connector or a fast custom function, and when I can show the gain."

## ⚠️ Common mistakes

> **⚠️ Watch out:** A UDF that ignores nulls. A null column value reaches your code, and a single `.trim` on it kills the task. Handle `null` explicitly or use built-in null-safe functions.

> **⚠️ Watch out:** Creating a connection, client or logger in the driver and using it inside a task. Create it inside the task, once per partition.

- Writing a UDF for logic that `lower`, `trim`, `when` or `regexp_extract` already express.
- Using Python UDFs for heavy row-level work without measuring, when a Pandas UDF or a JVM UDF would be much faster.
- Mixing Scala binary versions (`_2.12` and `_2.13`) in one build.
- Bundling Spark or Flink dependencies into a fat jar without marking them `provided`.
- Calling `.get()` on every Kafka send in a hot loop, which serializes the producer.
- Keying Kafka records with `null` when order per entity matters.

## ✅ Check yourself (quiz)

1. Why is a JVM UDF usually faster than a Python UDF for row-level logic in Spark?
   - A) Java is compiled and Python is not, so the code always runs faster
   - B) A JVM UDF runs inside the executor process, while a Python UDF requires serializing rows to a Python worker and back
   - C) JVM UDFs skip null checks
   - D) Python UDFs cannot read strings

<details><summary>Answer</summary>

**B** — the cost is the process boundary and serialization. A JVM UDF stays in the executor; a Python UDF moves each batch of rows to a separate Python process and back. Built-in functions are faster still.

</details>

2. In Kafka, what determines which partition a keyed record goes to?
   - A) The producer's `acks` setting
   - B) The record's key, by a hash (default partitioner), so the same key lands in the same partition
   - C) The consumer group name
   - D) The value's size

<details><summary>Answer</summary>

**B** — the default partitioner hashes the key. Same key, same partition, and therefore ordered delivery for that key within the partition.

</details>

3. What is the main reason the Scala `filter(_.amount > 6.0)` lambda is harder to optimize than `filter($"amount" > 6.0)`?
   - A) Lambdas cannot return booleans
   - B) The typed lambda is an opaque Scala function that Catalyst cannot inspect; the column expression is a tree Catalyst can rewrite and push down
   - C) Lambdas are always executed on the driver
   - D) Column expressions are compiled to Python

<details><summary>Answer</summary>

**B** — Catalyst works on expression trees. An arbitrary function is a black box, so predicates cannot be pushed into the source or reordered.

</details>

4. **(Senior)** A Flink job runs fine locally but fails on the cluster with `NoSuchMethodError` in a Jackson class. What is the likely cause and fix?

   - A) The job uses too much memory
   - B) The fat jar bundles a different Jackson version than the one the cluster provides, causing a classpath conflict; mark engine dependencies as `provided`, align versions, or relocate the conflicting package
   - C) Flink does not support Java libraries
   - D) The Kafka broker is too old

<details><summary>Answer</summary>

**B** — the local classpath and the cluster classpath differ. Engine-provided libraries should not be duplicated in the application jar, and conflicts are resolved by alignment or relocation.

</details>

5. **(Senior)** Your team wants to write a custom Scala UDF for a transformation that is currently done in Python with a Pandas UDF, and the job is slow. What do you check before you agree?

   - A) Nothing; JVM is always the right choice
   - B) First measure where the time goes (serialization, GC, the logic itself), check whether built-in functions or SQL express the logic, and compare the JVM UDF's cost, including build and deployment, against the measured gain
   - C) Convert the job to a single thread
   - D) Remove all UDFs and accept any runtime

<details><summary>Answer</summary>

**B** — the decision needs evidence. Often the fix is a built-in function, a better partitioning or fewer rows crossing the boundary. A JVM UDF is justified when measurements show the Python boundary is the bottleneck and built-ins cannot express the logic.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| Why are Spark, Flink and Kafka on the JVM? | They need threads, memory management and a mature runtime; their core code and connectors are Java/Scala |
| `Dataset[T]` vs `DataFrame` | Typed collection of T vs `Dataset[Row]` with column expressions optimized by Catalyst |
| Why avoid typed lambdas in hot paths? | Catalyst cannot see inside them, so predicates and projections cannot be optimized |
| Fastest order for row logic in Spark | Built-in functions, then JVM UDFs, then Pandas UDFs, then plain Python UDFs |
| What is `NotSerializableException` usually caused by? | A non-serializable object captured in a closure, often a connection created on the driver |
| Kafka partition from a key | Hash of the key (default partitioner): same key, same partition, ordered within it |
| What does `acks=all` do? | The producer waits for the in-sync replicas to acknowledge the write |
| What is the Flink job shape? | Source, then transformations (keyBy, windows, aggregates), then sink, then `execute` |

## 🔗 Go further

- **pyspark-api** shows the Python side of the same engine, so you can see what the JVM layer does for you.
- **spark-catalyst** explains the optimizer that rewards column expressions over opaque lambdas.
- **flink** and **kafka-streams** cover the streaming engines whose APIs appear in this lesson.
- **kafka-producers** goes deeper on batching, acks, idempotence and retries for producers.
- **os-jvm-essentials** covers the JVM runtime concepts (heap, GC, classloading) that explain most production failures here.
- Check the supported JDK and Scala versions for your exact Spark or Flink release before writing a build file.
