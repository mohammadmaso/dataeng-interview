---
term: "Vector database"
term_en: "Vector database"
aliases: ["vector database","vector store","پایگاه داده‌ی برداری","hnsw"]
category: ml
short: "مخزنی بهینه برای یافتن نزدیک‌ترین بردارها به بردار query."
related: ["embedding","database-index","sharding"]
---
## به زبان ساده (مبتدی)
کتابخانه‌ای که کتاب‌هایی با نزدیک‌ترین معنا به یک جمله را پیدا می‌کند، نه کتاب‌هایی که کلمات دقیق را دارند.

## نحوه‌ی کار (متوسط)
embeddingها را با ساختارهای نزدیک‌ترین همسایه‌ی تقریبی (گراف HNSW، IVF با product quantization) ایندکس می‌کند و فیلتر متادیتا دارد. مثال‌ها: pgvector، Pinecone، Milvus، Qdrant و OpenSearch.

## نگاه سنیور و نکات مصاحبه
ANN فراخوانی را با سرعت معاوضه می‌کند؛ ef/nprobe را تنظیم و recall را اندازه بگیرید. فیلتر همراه ANN می‌تواند recall یا latency را خراب کند. مصرف حافظه بزرگ است پس quantization و sharding مهم‌اند. در مقیاس متوسط اغلب pgvector کافی است.

سؤال مصاحبه: «نزدیک‌ترین همسایه‌ی دقیق در برابر تقریبی؟» پاسخ: دقیق همه چیز را اسکن می‌کند (O(N))؛ ایندکس ANN جستجوی زیرخطی با افت کم recall می‌دهد.
