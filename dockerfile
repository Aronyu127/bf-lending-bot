FROM timwarr/python3.11-talib

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV PORT=8501
ENV PYTHONUNBUFFERED=1
EXPOSE 8501

# Runs lending bot (start.py 1 = every minute) + Streamlit dashboard on PORT
CMD ["honcho", "start"]
