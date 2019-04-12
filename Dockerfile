FROM jupyter/pyspark-notebook

ENTRYPOINT jupyter notebook --ip=0.0.0.0 --allow-root

USER root

RUN apt-get update
RUN apt-get install -y vim
RUN apt-get install -y p7zip-full
RUN apt-get install -y curl
RUN apt-get install -y gnupg2

RUN pip install kafka-python
RUN pip install cassandra-driver

# install kafka
RUN wget https://www-eu.apache.org/dist/kafka/2.2.0/kafka_2.12-2.2.0.tgz
RUN tar xvzf kafka_2.12-2.2.0.tgz
RUN mv kafka_2.12-2.2.0 kafka
ENV PATH $HOME/kafka/bin:$PATH

# install cassandra
RUN echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list 
#RUN apt-key adv --keyserver pool.sks-keyservers.net --recv-key A278B781FE4B2BDA 
RUN curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add - 
RUN apt-get update 
RUN apt-get install -y cassandra 

# download the data
RUN mkdir data
RUN wget https://archive.org/download/stackexchange/codereview.stackexchange.com.7z -O data/temp.7z
RUN 7z e data/temp.7z -odata/
RUN rm data/temp.7z

# add init files
ADD init_kafka.sh .
ADD init_cassandra.sh .
ADD init_keyspace.sh .

# add notebooks
ADD kafka_consumer.ipynb .
ADD kafka_consumer_producer.ipynb .
ADD kafka_producer.ipynb .
ADD analysis.ipynb .