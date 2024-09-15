# Spark

## Spark Documentation

- it is recommended to see the complete documentation of spark
  beside this documentation

- documentations are in 2 main types
  - programming guide (concepts and what is it?)
  - api reference (practical functions with arguments)

## Concepts and Definitions

- spark

  - data-proecessing
  - large-scale (big data with paralellizability)
  - streaming (real-time data processing)

- spark should run on one cluster

  - standalone (just for development and debug)
  - yarn
  - mesos
  - kubernetes

- libraries in spark

  - sparksql
  - sparkpandas
  - mllib
  - graphx
  - structured streaming

- spark programming

  - python (pyspark library)
  - R
  - java
  - scala

- coding paradigms

  - running code files with spark-submit
  - working with sparkshell
  - working with jupyter-notebook

- processing types

  - batch (batch streaming)
    - aggregated data daily, weekly, monthly to process
    - delayed input, delayed output
    - example: anniversary report analysis
  - streaming (**_realtime_** streaming)
    - **_realtime_** data that comes for processing
    - continous input, continous output
    - example: fraud detection analysis

> in spark our code always is on batch (dataset).

- structure of spark streaming

  1. streaming input (ex: kafka, file writing)
  2. convert input streams to multiple micro batches
  3. batches go to the spark engine code
  4. output of spark engine as bathces written to output source (ex: kafka, ...)

- working with spark dataframe is similar to pandas and sql api functions

## API

- first of all you should create a `SparkSession`

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("Spark Streaming")
    .getOrCreate()
)
```

- note about the `master`, you should set your cluster master in
  this config, and above code runs simply with `python3 code.py`,
  but if you want to run with spark-submit you should set this config
  as submit configs.

- reading from source

  - batch
    ```python (file)
    df_raw = spark.read.format("text").load("datas/input.txt")
    ```
  - streaming (kafka)
    ```python
    df = spark.readStream.format("kafka") \
      .option("kafka.bootstrap.servers", "hostgateway") \
      .option("subscribe", "topicname") \
      .option("startingOffsets", "earliest") \
      .load()
    ```

- df = Dataframe

- see schematic

  ```python
  df.printSchema()
  ```

- see column types

  ```python
  df.dtypes
  ```

- see first rows

  ```python
  df.head([n])
  ```

- see all rows

  ```python
  df.show()
  ```

- see columns

  ```python
  df.columns
  ```

- see row counts

  ```shell
  df.count()
  ```

- filter table

  ```python
  df.filter(condition) # = df.where()
  ```

> `Column` Type is different from `Dataframe` Type!

- remove columns

  ```python
  df.drop("column1", "column2")
  ```

- add column

  ```python
  df.withColumn("colName", column)
  ```

  - example
    ```python
    df.withColumn("annual salary", df.salary * 12)
    ```

### Functions

- types

  - scaler
  - aggregate
  - generator

- you can define your own functions to use in spark in
  type of scaler and aggregation

#### Built-in Functions

```shell
from pyspark.sql.functions import *
```

- all of same operation on each property of column shoud be done here!

- [All Spark SQL Functions](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)

- you should use from functions and operations based on
  Column type

- Column Types

  - Numerical
    - ByteType
    - ShortType
    - IntegerType
    - LongType
    - FloatType
    - DoubleType
    - DecimalType
  - String
    - StringType
    - VarcharType(length)
    - CharType(length)
  - Binary
    - BinaryType
  - Boolean
    - BooleanType
  - Datetime
    - DateType
    - TimestampType
    - TimestampNTZType
  - Interval
    - YearMonthIntervalType(startField, endField)
    - DayTimeIntervalType(startField, endField)
  - Complex
    - ArrayType(elementType, containsNull)
    - MapType(keyType, valueType, valueContainsNull)
    - StructType(fields)
      - StructField(name, dataType, nullable)

- in functions ususally there is a `try_function` for returning `NULL`
  if operands and operators are incompatible. ex: `try_add`

- you can use from conversion functions to convert types to each other

- for example you can use from numerical operators on numerical types, ex:

  ```python
  annual_salary_col = (df.salary / 365) * 1.1
  ```

#### User Define Functions (UDFs, UDAFs)

- scaler udf is scaler type function
- example of scaler udf

  ```python
  from pyspark.sql.types import IntegerType

  slen = udf(lambda s: len(s), IntegerType())

  @udf
  def to_upper(s):
    if s is not None:
      return s.upper()

  @udf(returnType=IntegerType())
  def add_one(x):
    if x is not None:
      return x + 1

  df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))

  df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")).show()
  ```

- udaf = user define aggregation function
- example

  ```python
  from pyspark.sql.functions import col
  from pyspark.sql.types import DoubleType

  def calculate_average(group):
    total = sum(float(col(c)) for c in group.keys() if isinstance(col(c), float))
    count = len([c for c in group.keys() if isinstance(col(c), float)])
    return total / count if count > 0 else None

  average_udaf = udaf(calculate_average, DoubleType())

  data = [("A", 1.0), ("A", 2.0), ("B", 3.0)]
  df = spark.createDataFrame(data, ["group", "value"])
  result = df.groupBy("group").agg(average_udaf("value"))
  result.show()
  ```

### Streaming

- [Structured Streaming with Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
