FROM ubuntu:14.04

RUN apt-get update
RUN apt-get install -y build-essential wget software-properties-common

# install Java
RUN add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections && \
    echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections && \
    apt-get install -y oracle-java8-installer

# install mini-conda
RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh && \
    bash Miniconda2-latest-Linux-x86_64.sh -b && \
    rm Miniconda2-latest-Linux-x86_64.sh
ENV PATH /root/miniconda2/bin/:$PATH

# setup projects
RUN conda install -y numpy scipy
RUN cd home && \
    wget https://github.com/minhptx/iswc-2016-semantic-labeling/archive/v0.1.tar.gz && \
    mkdir semantic-labeling && \
    tar -xf v0.1.tar.gz -C semantic-labeling --strip-components=1 && \
    rm v0.1.tar.gz

RUN cd /home/semantic-labeling && \
    pip install -r requirements.txt && \

RUN mkdir /home/model && \
    mkdir -p /home/data/datasets && \
    mkdir /home/output

ENV PYTHONPATH /home/semantic-labeling:$PYTHONPATH
WORKDIR /home

