# Stage 1: Build React frontend
FROM node:20-slim AS frontend
WORKDIR /frontend
COPY frontend/package.json ./
RUN npm install
COPY frontend/ .
RUN npm run build

# Stage 2: Python FastAPI backend
FROM python:3.11-slim
WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/main.py .

# Copy React build output into static/ for FastAPI to serve
COPY --from=frontend /frontend/dist ./static

EXPOSE 8050

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8050", "--timeout-keep-alive", "120"]
