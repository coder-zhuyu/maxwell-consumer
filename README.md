# consumer MySQL binlog using maxwell and kafka

## status
It is currently in active development.

## maxwell
Maxwell reads MySQL binlogs and writes row updates as JSON to Kafka,
Kinesis, or other streaming platforms. Maxwell has low operational
overhead, requiring nothing but mysql and a place to write to. Its
common use cases include ETL, cache building/expiring, metrics collection,
search indexing and inter-service communication. Maxwell gives you some
of the benefits of event sourcing without having to re-architect your
entire platform.
http://maxwells-daemon.io/

  - maxwell using utc time, maybe you should modify it.
  https://github.com/coder-zhuyu/maxwell
  - run maxwell using docker
  ```bash
  docker run --name=maxwell -d maxwell:v1 bin/maxwell --user='xxxxxx' --password='xxxxxx' --host='xxx.xxx.xxx.xxx' --port=3306
  --replication_host='xxx.xxx.xxx.xxx' --replication_port=3306 --replication_user='xxx' --replication_password='xxx'
  --include_dbs=test --gtid_mode=true --producer=kafka --kafka.bootstrap.servers=192.168.10.29:9092,192.168.10.29:9093,192.168.10.29:9094
  --kafka_topic=maxwell --producer_partition_by=table --output_ddl=true
  ```

## consumer
  - config.py
  configure kafka, db, num of processes
  - consumer.py
  subscribe kafka, process sql to db, close kafka consumer.
  - graceful.py
  graceful stop all processes and exit.
  - logger.py
  logger
  - parse.py
  parse maxwell output json to sql, only support table which has primary key. support sql operations as:
    * insert
    translate to replace into
    * update
    using primary key
    * delete
    using primary key
    * create table
    * alter table
  - run.py
  main

## todo list
  1. pymysql reconnect when lost connection
  2. rebalance kafka partition