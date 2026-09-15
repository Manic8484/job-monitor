FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD exec gunicorn --bind :$PORT --workers 2 --threads 4 --timeout 0 app:app
