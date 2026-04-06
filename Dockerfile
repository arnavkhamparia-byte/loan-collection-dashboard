FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY dashboard_app.py db.py transforms.py ./

EXPOSE 8050

# Run with gunicorn (4 workers, timeout 120s for slow first queries)
CMD ["gunicorn", "--workers=4", "--timeout=120", "--bind=0.0.0.0:8050", "dashboard_app:server"]
