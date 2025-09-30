FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV TESSERACT_CMD=/usr/bin/tesseract


# Instalar Tesseract + español (+ inglés de respaldo)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-spa \
    tesseract-ocr-eng \
    libtesseract-dev \
    libleptonica-dev \
  && rm -rf /var/lib/apt/lists/*

# A veces ayuda declarar estas variables
ENV TESSDATA_PREFIX=/usr/share/tesseract-ocr/5/tessdata
ENV PATH="/usr/bin:${PATH}"

# Verificar instalación en build
RUN tesseract --version && tesseract --list-langs

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 10000
CMD ["python", "main.py"]
