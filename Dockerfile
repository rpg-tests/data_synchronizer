FROM python:3.9

RUN apt-get update && apt-get install -y netcat-traditional

ENV PYTHONUNBUFFERED 1

# Set working directory
WORKDIR /data-synchronizer-service

# Add source code to the working directory
ADD . /data-synchronizer-service

# Install all requirements 
RUN pip install -r requirements.txt

# Run server
CMD ["sh", "runserver.sh"]