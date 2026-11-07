FROM python:3.11-slim
WORKDIR /app
COPY p2p ./p2p
COPY peerProcess.py ./peerProcess.py
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /work
ENTRYPOINT ["python", "/app/peerProcess.py"]
