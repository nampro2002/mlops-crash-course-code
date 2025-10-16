FROM python:3.9-slim AS build

RUN apt-get update

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --upgrade "setuptools>=70" "wheel>=0.41" "packaging>=24.1"

COPY deployment/requirements.txt .
RUN pip install -r requirements.txt

FROM python:3.9-slim

COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY . /training_pipeline
WORKDIR /training_pipeline
