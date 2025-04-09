FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install elasticsearch requests

COPY *.py .

EXPOSE 5050

CMD ["python", "app.py"]