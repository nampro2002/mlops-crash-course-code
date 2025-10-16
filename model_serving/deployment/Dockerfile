FROM python:3.9-buster AS build

# RUN apt-get update

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
# pip<24.1 keeps support for legacy metadata used by feast==0.24.0
RUN pip install --upgrade "pip<24.1"

RUN pip install --upgrade "setuptools>=70" "wheel>=0.41" "packaging>=24.1"

COPY deployment/requirements.txt .
RUN pip install -r requirements.txt

FROM python:3.9-buster

COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY . /model_serving
WORKDIR /model_serving
