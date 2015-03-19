# Demo Cassandra access, dynamoDB and log process in Spark

This is a repackaged version of some working code extracted from a larger project I've worked on in 
the past. With my employer's permission, I am able to post them here. The code examples are to demonstrate
Spark instances that interact with Cassandra, DynamoDB and the log files.
 
All examples are ready to be run in AWS EC-2 Spark cluster.

There is a unified configuration file in `src/main/resources/application.conf`.

Helper scripts is provided to run database migration.


## Dependency

- SBT 0.13.7, sbt-assembly 0.12.0
- Spark 1.2.1
- Phantom 1.5.0 (Cassandra)
- Pillar 2.0.1 (Database migration)
- awscala 0.4.3 (AWS S3 and DynamoDB)

## Pre-build

- Put the correct AWS S3 credential and Cassandra config in the file  `src/main/resources/application.conf`

## Build

```
sbt assembly
```
This will create an uber jar that include all the example code.

## Cassandra and Spark example


### Run Cassandra setup and database migration first (Require the uber jar)

Create keyspace in C* by using the CQL script in `src/cql/create_keyspace.cql`

```
./script/pillar.sh -d src/main/resources/conf/pillar/migrations -e development initialize uu
./script/pillar.sh -d src/main/resources/conf/pillar/migrations -e development migrate uu
```

Run C* insert and read examples 

```
/path/to/spark-submit 
   --class com.acme.perf.{UsersUpsertPerf,UsersReadPerf} 
   --master local[2] 
   --driver-java-options "-Denv=development -Dapp.conf=src/main/resources/application.conf" 
   --conf "spark.executor.extraJavaOptions=-Denv=development -Dapp.conf=src/main/resources/application.conf" 
   target/scala-2.10/extract_0.0.1.jar  --date 2014-06-11
```

### The C* connector and data store code provided by Phantom requires Zookeeper in production. I modified
the connector code to strip out the Zookeeper dependency and solely based on the environment setting 
from the configuration file to ease the usage in dev, test, and production. It also cuts down quite some
dependency jars due to the elimination of the Zookeeper.
 

## DynamoDB examples

This requires pre-existing table in AWS DynamoDB.

Please refer to examples under `src/main/scala/com/acme/dynamo/`

## Some map reduce job against log files

Please refer to the example in `src/main/scala/com/acme/UserLog.scala`

