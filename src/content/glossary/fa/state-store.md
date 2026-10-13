---
term: "ذخیره‌ی state (State store)"
term_en: "State store (stateful stream processing)"
aliases: ["state store", "state backend", "keyed state", "stateful processing", "rocksdb"]
category: streaming
short: "ذخیره‌ی محلی و تحمل‌پذیر خطا که عملگر استریم بین رویدادها داده را در آن نگه می‌دارد، مثل شمارنده، پنجره و بافر join."
related: ["checkpoint", "exactly-once", "watermark"]
---
## به زبان ساده (مبتدی)
برای جواب دادن به «این مشتری امروز چند سفارش داده؟» کارگر باید سفارش‌های قبلی را به خاطر بسپارد. جایی که job استریم آنچه را باید به خاطر بسپارد نگه می‌دارد state است. سیستم این حافظه را منظم ذخیره می‌کند تا اگر ماشین بمیرد چیزی فراموش نشود.

## نحوه‌ی کار (متوسط)
عملگرهای stateless (map، filter) حافظه نمی‌خواهند. عملگرهای stateful (aggregate، پنجره، join، dedup، تشخیص الگو) state را پارتیشن‌شده بر اساس کلید نگه می‌دارند؛ هر task موازی state کلیدهای خود را دارد و محلی با latency کم به‌روز می‌کند. state در حافظه یا store توکار (RocksDB روی دیسک محلی) است و با checkpoint روی ذخیره‌سازی پایدار snapshot می‌شود؛ هنگام تغییر موازی‌سازی، key groupها بازتوزیع می‌شوند.

```
// Flink keyed state: count per user
public class Counter extends KeyedProcessFunction<String, Event, Out> {
  private ValueState<Long> count;
  public void open(Configuration c) {
    count = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Long.class));
  }
  public void processElement(Event e, Context ctx, Collector<Out> out) throws Exception {
    long n = (count.value() == null ? 0 : count.value()) + 1;
    count.update(n); out.collect(new Out(e.user, n));
  }
}
// state.backend: rocksdb (spills to local disk; incremental checkpoints to S3)
```

state باید محدود بماند: TTL، پنجره‌های بسته‌شونده با واترمارک و نمایش فشرده. Kafka Streams state را در storeهای محلی RocksDB با پشتیبان changelog topic نگه می‌دارد؛ Spark Structured Streaming نسخه‌های state هر batch را در checkpoint روی HDFS/S3 ذخیره می‌کند.

## نگاه سنیور و نکات مصاحبه
اندازه‌ی state همه‌چیز را تعیین می‌کند: مدت checkpoint، زمان بازیابی، حافظه و دیسک و هزینه‌ی تغییر موازی‌سازی. state بی‌کران (dedup بدون TTL، join بدون کران زمانی، sessionهایی که بسته نمی‌شوند) خرابی کلاسیک production است. کلیدهای داغ state skewed می‌سازند. تکامل schema در state سخت است: تغییر نوع state یا uid عملگر savepointها را بی‌اعتبار می‌کند؛ پس serialization (Avro/Protobuf) و idها را برنامه‌ریزی کنید. backend روی heap برای state کوچک و سریع، RocksDB برای state بزرگ (کندتر، serialization در هر دسترسی). queryable state یا lookup بیرونی (Redis) جایگزین‌های state محلی‌اند با هزینه‌ی latency و سازگاری.

سؤال مصاحبه: «checkpointهای job در Flink مدام کندتر می‌شوند؛ چرا؟» پاسخ: احتمالاً رشد state (بدون TTL، join/dedup بی‌کران) یا backpressure که barrierها را به تأخیر می‌اندازد؛ state را با TTL/واترمارک محدود، از checkpoint افزایشی RocksDB استفاده، skew را بررسی و unaligned checkpoint را در نظر می‌گیرم.
