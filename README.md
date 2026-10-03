# Midterm Data Pipeline

A **hybrid ELT (Extract–Load–Transform) data pipeline** built for a university Big Data practical midterm.  
Routes small files to **Python Batch**, large files to **Apache PySpark**, stores everything in **MongoDB**, and implements **PATH B: Incremental Loading and Advanced Reliability**.

---

## Architecture Overview

```
                    CSV INPUT
                        |
                        v
                 FILE DISCOVERY
                        |
                        v
                   FILE ROUTER
              (200 MB threshold)
                /              \
               v                v
        PYTHON BATCH          PYSPARK
        (≤ 200 MB)           (> 200 MB)
               \                /
                v              v
              orders_raw  (MongoDB)
                ELT: raw loaded FIRST
                        |
                        v
              QUALITY + CLEANING
              (10 deterministic rules)
                        |
           +------------+------------+
           |                         |
           v                         v
   orders_validated          orders_quarantine
   (VALID + CORRECTED)       (QUARANTINED)
   unique index: id_order
   Idempotent Upsert
           |
           v
   reports/results.json
```

---

## 🚀 Phase 2 (المشروع النهائي) - API, Aggregations, MVs, & Jobs

هذا القسم يغطي التحديثات التي تمت لتلبية متطلبات المشروع النهائي (7 درجات).

### 1. الإعداد والتشغيل (Installation & Setup)
تأكد من تثبيت الحزم المطلوبة وتجهيز البيئة:
```bash
# نسخ ملف الإعدادات
cp .env.example .env

# تثبيت المكتبات الجديدة (مثل FastAPI و Uvicorn و APScheduler)
pip install -r requirements.txt
```

### 2. تشغيل واجهة API الموحدة
تم توفير واجهة API متكاملة لتشغيل واختبار جميع الوظائف دون الحاجة لتشغيل سكربتات يدوية. لتشغيل السيرفر:
```bash
python -m uvicorn src.api:app --reload
```
👉 **بعد التشغيل، افتح الرابط التالي في المتصفح لتجربة كل شيء (Swagger UI):**  
**http://127.0.0.1:8000/docs**

### 3. الاستعلامات والفهارس (Queries, Indexes & Explain)
تم إنشاء 3 فهارس لتسريع البحث:
- `idx_customer_date` (فهرس مركب: `customer_id` + `order_date`)
- `idx_status` (فهرس على حالة الطلب)
- `idx_city` (فهرس على المدينة)

**الاستعلامات الـ 5 المتوفرة (عبر `GET /queries/{name}`):**
1. `customer_orders`: البحث عن طلبات عميل معين.
2. `quarantined_by_reason`: البحث عن الطلبات المرفوضة بسبب معين.
3. `orders_by_city_status`: البحث عن الطلبات في مدينة محددة وحالة محددة.
4. `top_valuable_orders`: استرجاع أغلى 10 طلبات.
5. `orders_in_date_range`: البحث ضمن نطاق زمني.

*(ملاحظة: يمكنك إضافة `?explain=true` لأي استعلام في واجهة Swagger لرؤية `executionStats` وإثبات استخدام الفهارس).*

### 4. التجميعات والتقارير (Aggregations)
يتوفر 5 تقارير تم بناؤها باستخدام `Aggregation Pipeline` (عبر `GET /aggregations/{name}`):
1. `sales_by_city`: المبيعات مجمعة حسب المدينة.
2. `top_products`: أفضل المنتجات مبيعاً.
3. `top_customers`: أفضل العملاء حسب حجم الإنفاق.
4. `sales_by_date`: إجمالي المبيعات مقسمة يومياً.
5. `orders_by_status`: توزيع الطلبات بناءً على حالتها.

### 5. العروض المادية (Materialized Views)
تم بناء عرضين (Views) يتم تحديثهما بآلية **التحديث التزايدي** (Incremental Update) باستخدام معامل `$merge` لتجنب مسح وبناء الجدول من الصفر:
- `mv_daily_sales_summary`
- `mv_top_products_summary`

*(يمكنك تحديثهما يدوياً عبر `POST /refresh-mv`).*

### 6. المهام المجدولة (Scheduled Jobs)
يستخدم النظام `APScheduler` لتشغيل مهام بالخلفية:
- مهمة تحديث الـ Materialized Views (كل ساعة).
- مهمة فحص صحة النظام (كل 30 دقيقة).

تقوم هذه المهام بتسجيل وقت البداية، النهاية، والنتيجة داخل جدول `jobs_log` في قاعدة البيانات. (يمكنك تشغيلها يدوياً للتجربة عبر `POST /jobs/{name}/run`).

---

## 🛠️ Phase 1 (المشروع النصفي) - Pipeline Execution

### تشغيل مسار معالجة البيانات (Ingestion)
```bash
# معالجة الملف الصغير (Python Batch)
python -m src.main --input data/orders_small_sample.csv

# معالجة الملف الضخم (PySpark)
python -m src.main --input data/orders_huge_mixed_quality.csv
```

### Path B (التحديث التزايدي للملفات)
```bash
python -m src.main --input data/orders_small_sample.csv --demo-path-b
```

### تشغيل الاختبارات الآلية
```bash
pytest tests/ -v
```

---

## تفاصيل قاعدة البيانات (Collections Schema)
- `orders_raw`: البيانات الخام قبل التنظيف.
- `orders_validated`: البيانات السليمة أو التي تم تصحيحها تلقائياً.
- `orders_quarantine`: البيانات المرفوضة مع توضيح سبب الرفض.
- `mv_daily_sales_summary`: عرض مادي للمبيعات اليومية.
- `mv_top_products_summary`: عرض مادي لأفضل المنتجات.
- `jobs_log`: سجل المهام المجدولة وحالتها.
