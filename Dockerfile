FROM python:3.9-slim

# Working directory set karein
WORKDIR /app

# System dependencies install karein
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Requirements copy aur install karein
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Baaki sara code copy karein
COPY . .

# Port expose karein (Back4App default 8080 use karta hai)
EXPOSE 8080

# App start karne ki command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

