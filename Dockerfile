FROM python:3.7
LABEL maintainer="Islam Elkadi, islam.elkadi.3261@gmail.com/ielkadi@ryerson.ca"
RUN apt-get -y update && apt-get -y install apt-utils freetds-dev freetds-bin    
RUN mkdir /app
WORKDIR /app
COPY . /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ENTRYPOINT ["python3","/app/app.py"]