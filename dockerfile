FROM ubuntu:22.04

WORKDIR /src

RUN apt update -y
RUN apt-get install iputils-ping -y
RUN apt install -y python3-pip
RUN apt install default-jre-headless -y


COPY SparkStream SparkStream

COPY requirements.txt .

COPY spark-redis_2.12-3.1.0-jar-with-dependencies.jar .

COPY main.py .

RUN pip install -r requirements.txt

CMD ["python3", "main.py"]
