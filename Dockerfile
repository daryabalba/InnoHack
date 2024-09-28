FROM python:latest

WORKDIR /scripts
COPY requirements.txt, main.py

RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
CMD ["python", "./main.py"]