# TODO update python version?
FROM python:3.13-slim
# RUN python -m pip install --upgrade pip

WORKDIR /app

COPY ./db/__init__.py ./db/
COPY ./db/storage.py ./db/
COPY ./.env .
COPY ./logging_setup.py .
COPY ./main.py .
COPY ./requirements.txt .
COPY ./whalefinder ./whalefinder/
COPY ./whales.py .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "main.py"]