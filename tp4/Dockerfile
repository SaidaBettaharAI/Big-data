#  اختيار صورة Python الأساسية
FROM python:3.10

#  تحديد مجلد العمل داخل الحاوية
WORKDIR /app

#  نسخ كل ملفات المشروع داخل الحاوية
COPY . .

# تثبيت المكتبات المطلوبة
RUN pip install pandas numpy matplotlib scikit-learn

# تشغيل سكريبت بايثون عند تشغيل الحاوية
CMD ["python", "bigdata3.py"]



