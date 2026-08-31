FROM python:3.11-slim
WORKDIR /app
COPY consumers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumers/anomaly_detector.py .
CMD ["python", "anomaly_detector.py"]