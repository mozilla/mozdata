ARG PYTHONTAG=2.7
FROM python:$PYTHONTAG AS packages
RUN rm -rf /var/cache/apt/archives && apt-get update && apt-get install --download-only -y libsnappy-dev openjdk-8-jre

FROM python:$PYTHONTAG
WORKDIR /app/python
COPY --from=packages /var/cache/apt/archives/*.deb /var/cache/apt/archives/
RUN dpkg -i /var/cache/apt/archives/*.deb
RUN pip install codecov
COPY python/setup.py python/VERSION /app/python/
RUN mkdir -p src/mozdata && pip install .[dev]
ENV AWS_ACCESS_KEY_ID=foo AWS_SECRET_ACCESS_KEY=bar PYTHONPATH=src
# fix for https://github.com/spulec/moto/issues/1793
RUN pip install 'boto3<1.8'
COPY python /app/python
COPY .git /app
ENTRYPOINT py.test --cov=src --flake8 && coverage xml && (codecov || true)
