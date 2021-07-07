FROM python:3.9-slim

COPY . /app
WORKDIR /app
RUN python3 -m pip install -r /app/requirement.txt

ENTRYPOINT ["python3", "/app/main.py"]
