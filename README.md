# traindb-prototype

TrainDB is a ML-based approximate query processing engine that aims to answer time-consuming analytical queries in a few seconds.
TrainDB will provide SQL-like query interface and support various DBMS data sources.

Currently, we are implementing a prototype for proof of concept.

## Requirements

* Java 8
* Maven 3.x

## Install

### Download

```console
$ git clone https://github.com/traindb-project/traindb-prototype.git
```

### Build

```console
$ cd traindb-prototype
$ mvn package
```

Then, you can find traindb-x.y.z-SNAPSHOT.tar.gz in traindb-assembly/target directory.

```console
$ tar xvfz traindb-assembly/target/traindb-0.1.0-SNAPSHOT.tar.gz
```

