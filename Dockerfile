FROM python:3.12-slim
ENV DEBIAN_FRONTEND=noninteractive TZ=Asia/Jerusalem PYTHONUNBUFFERED=1 POLYBOT_SUPERVISED=1
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 user
WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY --chown=user:user app/ /app/
COPY --chown=user:user supervisor.py /app/supervisor.py
# WORKDIR /app is created as root; the bot must be able to write trades.db, status, logs.
RUN chown user:user /app
USER user
ENV HOME=/home/user
EXPOSE 10000
ENV PORT=10000
CMD ["python", "/app/supervisor.py"]
