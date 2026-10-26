#!/bin/sh
set -e
python upload_genres.py
python run_queries.py
exec streamlit run docker_dashboard.py --server.port=8502 --server.address=0.0.0.0 --server.headless=true
