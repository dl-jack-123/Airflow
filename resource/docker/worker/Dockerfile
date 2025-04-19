FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# 設定工作目錄
WORKDIR /app

# 安裝系統依賴
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 複製 requirements.txt
COPY requirements.txt .


# 使用 uv 安裝 Python 套件
RUN uv pip install --no-cache -r requirements.txt --system

# 設定預設命令
CMD ["python"] 