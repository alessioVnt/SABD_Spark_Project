# SABD_Spark_Project

Gruppo formato da: Vintari Alessio, Menzolini Luca

All'interno della repository è possibile trovare tutti i file prodotti per il progetto.

Sono stati usati ai fini del progetto: NiFi, Kafka, Flume, HDFS (container), Spark, MongoDB(container).

Passi per il correto funzionamento del progetto:

NiFi:
1- inserire all'interno della directory dove è installato NiFi le cartelle VintariMenzoliniSABD/NiFi/tmp e VintariMenzoliniSABD/NiFi/data ed il loro contenuto

2- avviare NiFi con il comando:   sudo ./bin/nifi.sh start

3- caricare il template contenuto in VintariMenzoliniSABD/NiFi/NiFiTemplate

Kafka:
1- Per avviare Zookeeper e Kafka, eseguire dalla cartella dove è installato Kafka, i comandi:
    1- bin/zookeeper-server-start.sh config/zookeeper.properties
    2- bin/kafka-server-start.sh config/server.properties
    
HDFS:
per HDFS è stata utilizzata l'immagine fornita a laboratorio effeerre/hadoop.
Per avviare HDFS eseguire i comandi presenti in scaletta.txt fornita a lezione (assicurandosi di aggiungere al master l'opzione -p54310:54310):

  sudo docker network create --driver bridge hadoop_network
  
  sudo docker run -t -i -p 9864:9864 -d --network=hadoop_network --name=slave1 effeerre/hadoop
  
  sudo docker run -t -i -p 9863:9864 -d --network=hadoop_network --name=slave2 effeerre/hadoop
  
  sudo docker run -t -i -p 9862:9864 -d --network=hadoop_network --name=slave3 effeerre/hadoop
  
  sudo docker run -t -i -p 9870:9870 -p54310:54310 --network=hadoop_network --name=master effeerre/hadoop

fatto ciò dal terminale del master eseguire:

  hdfs namenode -format
  
  $HADOOP_HOME/sbin/start-dfs.sh
  
  hdfs dfs -mkdir /topics
  
  hdfs dfs -mkdir /output
  
  hdfs dfs -mkdir /output/query1
  
  hdfs dfs -mkdir /output/query2
 
  hdfs dfs -mkdir /output/query3
  
  hdfs dfs -chmod -R 777 /
  
  hdfs dfs -chown -R *NOMEUTENTE* /

Flume:

1- inserire all'interno della cartella conf, che è possibile trovare nella directory dove flume è installato, il contenuto della cartella VintariMenzoliniSABD/Flume

2- dalla directory dove è installato flume eseguire il comando: bin/flume-ng agent -n agent -c conf -f conf/flume-conf.properties -Dflume.root.logger=INFO,console

Questo conclude la fase di data ingestion.
Prima di andare ad eseguire le query tramite Spark assicurarsi di inizializzare mongoDB (è stata utilizzata l'immagine ufficiale ed i comandi visti a laboratorio):

1- docker pull mongo

2- docker network create mongonet

3- docker run -i -t -p 27017:27017 --name mongo_server --network=mongonet mongo:latest /usr/bin/mongod --smallfiles --bind_ip_all

Nel template di NiFi caricato in precedenza sono presenti anche i processors che si occuperanno di portare i dati da HDFS a MongoDB.

A questo punto eseguire il codice Java contenuto nella repository per le varie query con Spark:

- Main class query 1: src/main/java/Query1

- Main class query 2: src/main/java/Query2

- Main class query 3: src/main/java/Query3

  

 
