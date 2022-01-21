FROM python:alpine
ADD . /app
RUN pip install beautifulsoup4 requests pymongo redis
WORKDIR /app
CMD python main.py