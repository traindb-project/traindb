# TrainDB

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
$ git clone https://github.com/traindb-project/traindb.git
```

### Build

```console
$ cd traindb
$ mvn package
```

Then, you can find traindb-x.y.z-SNAPSHOT.tar.gz in traindb-assembly/target directory.

```console
$ tar xvfz traindb-assembly/target/traindb-0.1.0-SNAPSHOT.tar.gz
```

To use ML models, you need to checkout models.\
For python environment setup, see README in our [traindb-model](https://github.com/traindb-project/traindb-model) repository.
``` console
$ cd traindb-assembly/target/traindb-0.1.0-SNAPSHOT
$ svn co https://github.com/traindb-project/traindb-model/trunk/models
```

## Run

### Example

Now, you can execute SQL statements using the command line interface.\
You need to put JDBC driver for your DBMS into the directory included in CLASSPATH.

```console
$ cd traindb-assembly/target/traindb-0.1.0-SNAPSHOT
$ bin/trsql
sqlline> !connect jdbc:traindb:<dbms>://<host>
Enter username for jdbc:traindb:<dbms>://localhost: <username> 
Enter password for jdbc:traindb:<dbms>://localhost: <password>
0: jdbc:traindb:<dbms>://<host>>
```

You can train ML models and run approximate queries like the following example.
```
0: jdbc:traindb:<dbms>://<host>> CREATE MODEL tablegan TYPE SYNOPSIS LOCAL AS 'TableGAN' in '$TRAINDB_PREFIX/models/TableGAN.py';
No rows affected (0.255 seconds)
0: jdbc:traindb:<dbms>://<host>> TRAIN MODEL tablegan INSTANCE tgan ON <schema>.<table>(<column 1>, <column 2>, ...);
epoch 1 step 50 tensor(1.1035, grad_fn=<SubBackward0>) tensor(0.7770, grad_fn=<NegBackward>) None
epoch 1 step 100 tensor(0.8791, grad_fn=<SubBackward0>) tensor(0.9682, grad_fn=<NegBackward>) None
...
0: jdbc:traindb:<dbms>://<host>> CREATE SYNOPSIS <synopsis> FROM MODEL INSTANCE tgan LIMIT <# of rows to generate>;
...
0: jdbc:traindb:<dbms>://<host>> SELECT avg(<column>) FROM <synopsis>;
```
