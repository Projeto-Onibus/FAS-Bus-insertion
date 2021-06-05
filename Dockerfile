#
# FAS-Bus's insertion module of Rio de Janeiro's fleet.
# Part of project FAS-Bus
# More details at README.md
# 

FROM python:3.8-buster

# programs to 
RUN apt-get update && apt-get -y install cron wget tar

# Install dependencies for populate database
RUN python3 -m pip install psycopg2 pandas numpy requests tables

# Creates necessary directories
RUN mkdir /collect_bus && mkdir /collect_bus_old && mkdir /collect_line && touch /cron.log && touch /collect_bus_old/collection_logs.csv

# Copy hello-cron file to the cron.d directory
COPY app/dbinsertion.cron /etc/cron.d/dbinsertion.cron
 
# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/dbinsertion.cron

# Apply cron job
RUN crontab /etc/cron.d/dbinsertion.cron

# Move app to container
COPY ./app/* /app/

WORKDIR /app

RUN chmod +x *.sh

# Run the command on container startup
CMD cron -f