FROM python:3.8-slim

WORKDIR /app

COPY ./data ./data/
COPY ./db ./db/
COPY ./whalefinder ./whalefinder/

COPY ./config.json .
COPY ./main.py .
COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "main.py"]