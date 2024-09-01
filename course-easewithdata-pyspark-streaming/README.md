# Ease with Data Course - Spark Streaming with Pyspark

- score to course: 78/100

## ep 01

- all things about structured streaming
- convert spark batch code to streaming code
- realtime code with kafka, redis & cosmosDB
- realtime code with different file formats
- handle event-time and late data scenarios
- performance optimization

## ep 02

- structured streaming basics

  - input sources (what)
  - triggering (when)
  - process data (how)
  - output sources (where)

- processing types

  - batch (batch streaming)
    - aggregated data daily, weekly, monthly to process
    - delayed input, delayed output
    - example: anniversary report analysis
  - streaming (realtime streaming)
    - realtime data that comes for processing
    - continous input, continous output
    - example: fraud detection analysis

- structure of spark streaming
  1. streaming input (ex: kafka, file writing)
  2. convert input streams to multiple micro batches
  3. batches go to the spark engine
  4. output of spark engine as bathces written to output source (ex: kafka, ...)

## ep 03

- setup environment

  - install docker
  - clone the docker images from `https://github.com/subhamkharwal/docker-images.git`
  - `docker compose up`
  - from compose up logs get the token of login for jupyter notebook
  - from `docker exec` command go to the `kafka` container

    - below commands

      ```shell
      kafka-topics --list --bootstrap-server ed-kafka:9092
      ```

      ```shell
      kafka-topics --create --topic ay-topic --bootstrap-server ed-kafka:9092
      ```

      ```shell
      kafka-topics --list --bootstrap-server ed-kafka:9092
      ```

## ep 04

- reading from socket

  - below commands

    ```shell
    docker exec -it <jupyter-container> /bin/bash
    ```

    ```shell
    sudo apt-get update
    ```

    ```shell
    sudo apt-get install ncat
    ```

    ```shell
    ncat -v
    ```

  - go to jupyter lab
