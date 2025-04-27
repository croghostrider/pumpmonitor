FROM python:3.12-slim

# Arbeitsverzeichnis
WORKDIR /app

# Systemabhängigkeiten (falls nötig)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Abhängigkeiten kopieren und installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Quellcode kopieren
COPY src/ ./src/

# Standard-Command
CMD ["python", "src/monitor.py"]
