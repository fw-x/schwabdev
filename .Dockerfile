# ==========================================
# STAGE 1: The Builder (Compiles everything)
# ==========================================
FROM python:3.11-slim-bookworm AS builder

ARG APP_NAME
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

# Copy and compile wheels
COPY ./${APP_NAME}/requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /build/wheels -r requirements.txt


# ==========================================
# STAGE 2: The Production & Development Runtime
# ==========================================
FROM python:3.11-slim-bookworm

ARG APP_NAME
ARG USER_ID=1000
ARG GROUP_ID=1000

ENV PYTHONUNBUFFERED=1 \
    PYTHONTDONTWRITEBYTECODE=1 \
    HOME=/home/appuser

ENV PYTHONPATH=$HOME
    

# 1. Create group and user
RUN groupadd -g ${GROUP_ID} appgroup && \
    useradd -u ${USER_ID} -g appgroup -m -d /home/appuser appuser

# 2. Set the working directory INSIDE the user's home
WORKDIR /home/appuser/${APP_NAME}

# 3. Create app-specific directories and ensure ownership
RUN mkdir -p /home/appuser/.duckdb

# 4. Install pre-compiled wheels from Stage 1
COPY --from=builder /build/wheels /wheels
RUN pip install --no-cache-dir /wheels/* && rm -rf /wheels

# 5. COPY your actual application code (Fallback for Prod/Testing)
# For local dev, your docker-compose bind-mount will override this.
COPY ./${APP_NAME} .

# 6. Fix ALL permissions in the home directory at once
RUN chown -R appuser:appgroup /home/appuser

USER appuser

# Default command (adjust to your entrypoint/app)
# CMD ["python", "main.py"]