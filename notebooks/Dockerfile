FROM jupyter/scipy-notebook:latest

COPY ./requirements.txt /home/jovyan/work/

RUN pip install --no-cache-dir -r ./work/requirements.txt

ENTRYPOINT [ "start-notebook.py" ]