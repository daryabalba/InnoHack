FROM python:latest

WORKDIR /trying/hack-docker/scripts
COPY . .

RUN python -m pip install --upgrade pip
RUN pip install -r ./requirements.txt
CMD ["python", "./main.py"]