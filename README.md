<div align="center">
  <h1>Apache Kafka Learning</h1>
  <p><i>Kafka APIs with Java</i></p>
</div>

This repository uses OpenJDK 11.

## Wikimedia Event Change Data Stream

The project implements a Java Application that produces, data from the [Wikimedia EventStream API](https://stream.wikimedia.org/v2/ui/#/?streams=mediawiki.recentchange) to a local topic.

The Application then consumes the data from the topic and sends to a local instance of OpenSearch.

## Kafka Basics

---

### Environment Setup

To execute the code for Producers and Consumers inside `kafka-basics/`, it is necessary to have an instance of Zookeeper, at `localhost:2181`, and Kafka running in your localhost at port `localhost:9092`.

### Docker Containers

Environment setup is already configured in the `docker-compose.yml` dockerfile.

Inside the project's folder, run:

```console
docker compose up
```

This will pull the images from Docker Hub and start the required services.

---

### Topics

A topic called `demo.java.topic` should also be created with at least 3 partitions. This allows to observe the logs and see Kafka's behavior to assign, manage and rebalance partitions.

Scripts to execute the commands below are available in the `bin/` folder of the Apache Kafka [Binary Download](https://kafka.apache.org/downloads)

#### Create Topics

```console
kafka-topics.sh --bootstrap-server localhost:9092 --topic demo.java.topic --create --partitions 3 --replication-factor 1
```

#### List Topics

```console
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

#### Describe Topics

```console
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic demo.java.topic
```

#### Delete Topics

```console
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic demo.java.topic
```

There are also ways to start Producers and Consumers from the CLI tools.

More instructions are available [here](https://www.conduktor.io/kafka/kafka-cli-tutorial).

---

## Kafka Theory Roundup

From the Conduktor's Udemy Course where I'm getting this knowledge from...

![image](https://user-images.githubusercontent.com/63078965/232113781-91e0d1c0-4d26-45f7-9c65-c3e4ae3127e3.png)
