# traindb-prototype

TrainDB is a ML-based approximate query processing engine that aims to answer time-consuming analytical queries in a few seconds.
TrainDB will provide SQL-like query interface and support various DBMS data sources.

Currently, we are implementing a prototype for proof of concept.

## Requirements

* Java 8+
* Maven 3.x
* SQLite3

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

To use ML models, you need to checkout models.\
For python environment setup, look at README in our [traindb-model](https://github.com/traindb-project/traindb-model) repository.
``` console
$ cd traindb-assembly/target/traindb-0.1.0-SNAPSHOT
$ svn co https://github.com/traindb-project/traindb-model/trunk/models
```

## Run

### Example

```console
$ cd traindb-assembly/target/traindb-0.1.0-SNAPSHOT
$ bin/trsql
sqlline> !connect jdbc:traindb:<dbms>://<host>
Enter username for jdbc:traindb:<dbms>://localhost: <username> 
Enter password for jdbc:traindb:<dbms>://localhost: <password>
0: jdbc:traindb:<dbms>://<host>>
```

Now, you can execute SQL statements using the command line interface.\
You need to put JDBC driver for your DBMS into the directory included in CLASSPATH.
