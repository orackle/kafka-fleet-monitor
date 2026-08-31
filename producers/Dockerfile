FROM python:3.11-slim
WORKDIR /app
COPY producers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producers/node_agent.py .
ENTRYPOINT ["python", "node_agent.py"]