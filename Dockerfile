#
# FAS-Bus's insertion module of Rio de Janeiro's fleet.
# Part of project FAS-Bus
# More details at README.md
# 

FROM python:3.8-buster as base

# basic libraries
RUN apt-get update && apt-get -y install wget tzdata git

# Install dependencies for populate database
RUN python3 -m pip install psycopg2 requests schedule

# Clones repo
RUN mkdir /app

WORKDIR /app

RUN git clone https://github.com/Projeto-Onibus/FAS.git


CMD ["python3","-m","FAS.insertion.Inserter","--database-credentials","/run/secrets/credentials.env","-r"]
