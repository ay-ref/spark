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

  - go to jupyter lab and write your streaming codes
  - go to jupyter container terminal and below commands (run ncat)
    ```shell
    ncat -l 4545
    ```
  - run `writeStream` in your jupyter code
  - now you can input to the ncat terminal
    and see the result in jupyter container logs

## ep 05

- spark streaming output modes
  - "append" (default)
    - write just new micro batches and previous micro batches are gone
    - useful when you don't need previous micro batches in you processing sequence
  - "complete"
    - write all of rows updated and not updated
  - "update"
    - just write updated rows
    - not supported by all output sources

## ep 06

- data processing architecture
  - lambda
    - seperating the batch processing from the streaming processing
  - kappa
    - converting batch data to micro batches for having just one streaming processing

## ep 07

- one of source for spark for real-time processing
  are files, you can add a new file or write into files in real-time like before

## ep 08

- for seeing the sequence of spark streaming processing you should create a checkpointLocation in config
  and then it makes some structured files in it
- files are like below:
  - commits/
    - show the outputed micro batches
      with file number of batch number
    - in the files you can see the details
      of batch like input source that used
      in processing for output batch
    - commits are very important for fault-tolerance becuase if we remove the
      last commit the spark goes on from
      previous last commit and do something
      again (for example if a file removed and again added with same name spark in natural does not consider that but if you remove the last commit it consider that)
