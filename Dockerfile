FROM python:3.11-slim

WORKDIR /app

# Copy shared directory
COPY shared /app/shared

# Copy service files
COPY services/integration-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/integration-service/app /app/app

EXPOSE 8015

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8015"]
