# Dockerfile
FROM python:3.11-slim

# Evita prompts y acelera pip
ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ---- SO + Tesseract (es/eng) ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr tesseract-ocr-spa tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*

# pytesseract usará esta ruta
ENV TESSERACT_CMD=/usr/bin/tesseract

# ---- App ----
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Render expone $PORT; usa 10000 por defecto local
ENV PORT=10000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
