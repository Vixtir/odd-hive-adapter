FROM python:3.6
RUN apt-get update -y && \
    apt-get install -y libsasl2-modules libsasl2-dev
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY server /srv/app
RUN useradd --create-home --shell /bin/bash appuser
USER appuser
ENV PYTHONUNBUFFERED=1
ENV PATH=/home/appuser/.local/bin:$PATH
EXPOSE 8080
WORKDIR /srv/app/
ENTRYPOINT gunicorn --bind 0.0.0.0:8080 --workers=1 wsgi:application