FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y software-properties-common curl \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y python3.11 python3.11-venv python3.11-dev python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt || true
RUN apt-get upgrade -y
RUN pip install playwright
RUN playwright install-deps
RUN playwright install
CMD tail -f /dev/null
