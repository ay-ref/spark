# PySpark Full Course - Free Code Camp Course

## Introduction

- the problem begins from when we have a very huge amount of data
like 64 GiB data to do some process on them and after one threshold
of amount of data we can't do vertical scaling of system and need
horizontal scaling meaning to create a distributed network of systems
to do process of the porject.

- in this course we use from spark in pythion by pyspark library interface.

- apache spark sections
  - spark sql
  - spark streaming
  - spark mlib
  - spark graphx

- spark runs on (distributed management systems)
  - standalone
  - hadoop
  - kubernetes
  - apache mesos
  - cloud

## Installation

> you should check your python version compatible
> with your spark version that you want!\
> spark installation usually has many "not useful" challange!

1. create virtual environment and activate

    ```shell
    python -m venv myenv
    activate myenv/bin/activate
    ```

1. install pyspark in your virtual environment

    ```shell
    pip install pyspark
    ```
