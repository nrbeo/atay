FROM python:3.13-slim

WORKDIR /app

# Install build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY docker/backend/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy backend source code
COPY src/backend ./src/backend

EXPOSE 8000

# Run FastAPI server
CMD ["uvicorn", "src.backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
