# Use an official Python slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY app.py .
COPY api ./api
COPY data ./data

# Expose the port the app listens on
EXPOSE 8000

# Run gunicorn as the production WSGI server. 
# Added threads and timeout to handle slow IO operations (OpenAI & Telegram API calls)
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", "--timeout", "120", "--access-logfile", "-", "--error-logfile", "-", "app:app"]