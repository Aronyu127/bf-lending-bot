bot: python start.py 1
# Honcho bumps PORT by +100 per process type; use STREAMLIT_SERVER_PORT for the real listen port (see docker-compose).
dashboard: streamlit run dashboard.py --server.port ${STREAMLIT_SERVER_PORT:-$PORT} --server.address 0.0.0.0
