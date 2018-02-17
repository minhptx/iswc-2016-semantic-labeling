FROM continuumio/miniconda

RUN apt-get update

RUN cd home && \
    wget https://github.com/minhptx/iswc-2016-semantic-labeling/archive/v0.1-alpha.tar.gz && \
    mkdir semantic-labeling && \
    tar -xf v0.1-alpha.tar.gz -C semantic-labeling --strip-components=1 && \
    rm v0.1-alpha.tar.gz

RUN conda install numpy scipy
RUN pip install -r requirements.txt

WORKDIR /home/semantic-labeling
