# Company Enrichment Tool - Docker Image
# 
# Build: docker build -t company-enricher .
# Run:   docker-compose up -d
#
# For initial database import, run:
#   docker-compose run --rm app python import_csv.py /data/companies.csv

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
COPY database.py .
COPY import_csv.py .
COPY update_from_csv.py .
COPY static/ ./static/

# Create data directory for SQLite database and CSV files
RUN mkdir -p /data

# Environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=production
ENV DB_PATH=/data/companies.db
ENV CSV_PATH=/data/BasicCompanyDataAsOneFile.csv

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/ || exit 1

# Run the application with gunicorn for production
CMD ["python", "-m", "gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--timeout", "120", "app:app"]

