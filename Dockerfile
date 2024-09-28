FROM python:latest

WORKDIR /trying/hack-docker/scripts


COPY ./scripts/requirements.txt ./
COPY ./scripts/main.py ./
COPY ./scripts/toolkit/ ./toolkit/


RUN python -m pip install --upgrade pip
RUN pip install -r ./requirements.txt
CMD ["python", "./main.py"]