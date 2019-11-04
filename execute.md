## Server

```
    python log_producer.py | nc -lk 6666
```

## Listenser

```
    nc -v localhost 6666
```

## Spark

```
    spark-submit spark_process.py localhost 6666
```