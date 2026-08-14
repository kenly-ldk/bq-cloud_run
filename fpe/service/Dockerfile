FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

# Worker model comes from gunicorn.conf.py, which reads FPE_WORKERS /
# FPE_THREADS / FPE_WORKER_CLASS at container start — so a sweep can change
# concurrency behaviour by redeploying the same image with different env.
CMD ["gunicorn", "--config", "gunicorn.conf.py", "main:app"]
