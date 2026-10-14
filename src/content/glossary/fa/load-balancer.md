---
term: "Load balancer"
term_en: "Load balancer"
aliases: ["load balancer","توزیع‌کننده‌ی بار"]
category: infra
short: "مؤلفه‌ای که درخواست‌های ورودی را بین چند سرور پخش می‌کند."
related: ["consistent-hashing","sharding","replication"]
---
## به زبان ساده (مبتدی)
میزبان رستورانی که هر گروه جدید را به میزی با صندلی خالی می‌فرستد تا هیچ گارسونی زیر بار نرود.

## نحوه‌ی کار (متوسط)
راهبردها: round robin، least connections، مبتنی بر hash (چسبیده به کلید) و وزن‌دار. لایه‌ی ۴ TCP را متعادل می‌کند؛ لایه‌ی ۷ HTTP را می‌فهمد و می‌تواند بر اساس مسیر یا header مسیریابی کند. health check سرورهای خراب را حذف می‌کند.

## نگاه سنیور و نکات مصاحبه
خود balancer نباید single point of failure باشد (جفت‌های افزونه، DNS یا anycast). consistent hashing جابه‌جایی را هنگام تغییر سرورها کم می‌کند. برای backendهای stateful (cache، shard) مسیریابی بر اساس کلید بهتر است.

سؤال مصاحبه: «L4 در برابر L7؟» پاسخ: L4 سریع‌تر و مستقل از پروتکل است؛ L7 مسیریابی محتوایی، TLS termination و retry را با هزینه‌ی بیشتر ممکن می‌کند.
