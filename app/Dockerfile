FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY docker_dashboard.py upload_genres.py entrypoint.sh .
RUN chmod +x entrypoint.sh
EXPOSE 8502
CMD ["./entrypoint.sh"]
