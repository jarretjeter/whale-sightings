FROM jupyter/scipy-notebook:latest

COPY ./requirements.txt /home/jovyan/work/
COPY ./data/ne_110m_admin_0_countries/ /home/jovyan/work/ne_110m_admin_0_countries/

RUN pip install --no-cache-dir -r ./work/requirements.txt

ENTRYPOINT [ "start-notebook.py" ]