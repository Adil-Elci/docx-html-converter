FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN mkdir -p /opt/venv/bin \
    && ln -sf /usr/local/bin/python /opt/venv/bin/python \
    && ln -sf /usr/local/bin/alembic /opt/venv/bin/alembic

COPY . /app
RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/app/entrypoint.sh"]
