FROM ubuntu:14.04

RUN apt-get update
RUN apt-get install -y build-essential wget software-properties-common

RUN wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh && \
    bash Miniconda2-latest-Linux-x86_64.sh -b

ENV PATH /root/miniconda2/bin/:$PATH

RUN /root/miniconda2/bin/conda install -y numpy scipy

RUN cd home && \
    wget https://github.com/minhptx/iswc-2016-semantic-labeling/archive/v0.1-alpha.3.tar.gz && \
    mkdir semantic-labeling && \
    tar -xf v0.1-alpha.3.tar.gz -C semantic-labeling --strip-components=1 && \
    rm v0.1-alpha.3.tar.gz

RUN pip install -r requirements.txt

RUN apt-get install -y
RUN add-apt-repository ppa:webupd8team/java -y
RUN apt-get update
RUN echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections && \
    echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
RUN apt-get install oracle-java8-installer

WORKDIR /home/semantic-labeling

