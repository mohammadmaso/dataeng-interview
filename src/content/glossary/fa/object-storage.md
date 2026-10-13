---
term: "ذخیره‌سازی شیء (Object storage)"
term_en: "Object storage"
aliases: ["object storage", "s3", "gcs", "adls", "minio", "blob storage"]
category: infra
short: "ذخیره‌سازی کلید-مقدار تخت، مقیاس‌پذیر و بادوام برای اشیای تغییرناپذیر (S3، GCS، ADLS) که پایه‌ی data lake با محاسبه‌ی جداست."
related: ["data-lake", "lakehouse", "parquet"]
---
## به زبان ساده (مبتدی)
object storage یک انبار غول‌پیکر و ارزان با قفسه‌های امانی است: کل یک فایل را با یک برچسب (کلید) تحویل می‌دهید و بعداً با همان برچسب پس می‌گیرید. نمی‌توانید یک صفحه‌ی داخل فایل را ویرایش کنید؛ کل فایل را جایگزین می‌کنید. مقدار باورنکردنی داده را نگه می‌دارد و ماشین‌های زیادی هم‌زمان می‌توانند بخوانند.

## نحوه‌ی کار (متوسط)
فضای نام تخت از bucket و کلید با اشیای تغییرناپذیر که با HTTP دسترسی می‌شوند. دوام و مقیاس بسیار بالا با هزینه‌ی کم می‌دهد و throughput از درخواست‌های موازی و ranged GET می‌آید (همان‌طور که خواننده‌های Parquet footer و column chunkها را می‌گیرند). ذخیره‌سازی از محاسبه جداست و کلاسترها مستقل ساخته یا نابود می‌شوند.

```
s3://bucket/warehouse/events/dt=2024-05-01/part-0001.parquet     <- "folders" are just key prefixes

Operations:  PUT / GET (range reads!) / DELETE / LIST (prefix, paginated, slow at scale)
No in-place update, no append (traditionally), NO atomic rename of a "directory"
Durability ~11 nines via replication/erasure coding; cost: storage + requests + egress
```

تفاوت با فایل‌سیستم: دایرکتوری واقعی نیست (prefix)، rename ارزان نیست (کپی به‌علاوه‌ی حذف و غیراتمیک)، `LIST` کند و صفحه‌بندی‌شده است، latency هر درخواست ده‌ها میلی‌ثانیه است و هزینه شامل درخواست و egress است. S3 مدرن سازگاری قوی read-after-write دارد. table formatها عمدتاً برای جبران همین‌ها هستند: commit اتمیک با اشاره‌گر متادیتا به‌جای rename و فایل manifest به‌جای list.

## نگاه سنیور و نکات مصاحبه
رویه‌های کارایی: فایل‌های بزرگ‌تر (صدها مگابایت)، فایل کمتر در هر کوئری، خواندن موازی، خواندن ranged با column pruning، پرهیز از prefixهای داغ در سیستم‌های قدیمی‌تر و هم‌مکان کردن محاسبه در همان region برای پرهیز از egress و تأخیر. قابلیت‌های عملیاتی: قوانین lifecycle (انتقال به لایه‌ی سرد، انقضا)، versioning، replication، رمزنگاری، دسترسی با IAM و credential کوتاه‌مدت. تله‌های هزینه: میلیون‌ها فایل ریز (هزینه‌ی درخواست)، انتقال بین region و bucket و نسخه‌های فراموش‌شده. مقایسه با HDFS: locality داده و معنای شبیه POSIX در برابر elasticity و هزینه؛ بیشتر پلتفرم‌های مدرن به object storage رفتند.

سؤال مصاحبه: «چرا نمی‌شود روی S3 با rename commit امن کرد و چه چیزی جایگزین شد؟» پاسخ: S3 rename اتمیک دایرکتوری ندارد (کپی به‌علاوه‌ی حذف)، پس output committer ممکن است نتیجه‌ی ناقص نشان دهد؛ table formatهایی مثل Iceberg/Delta فایل تغییرناپذیر می‌نویسند و با تعویض اتمیک اشاره‌گر متادیتا commit می‌کنند.
