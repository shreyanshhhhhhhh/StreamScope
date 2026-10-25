FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY docker_dashboard.py .
EXPOSE 8502
CMD ["streamlit", "run", "docker_dashboard.py", "--server.port=8502", "--server.address=0.0.0.0", "--server.headless=true"]
