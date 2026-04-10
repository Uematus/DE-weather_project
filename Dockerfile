FROM python:3.12-slim

# Системные зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Устанавливаем Bruin CLI
RUN curl -L https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | sh && \
    mv /root/.local/bin/bruin /usr/local/bin/bruin

# Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app

CMD ["sleep", "infinity"]
