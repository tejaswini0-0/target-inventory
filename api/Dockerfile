FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install kafka-python==2.0.2
COPY ./app .
RUN python setup_db.py || true
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]