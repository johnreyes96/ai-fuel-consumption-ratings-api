# docker/dockerfile:1

FROM python:3.10.0

WORKDIR /usr/python-docker

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000

ENTRYPOINT ["python", "src/main/python/regression/main.py"]