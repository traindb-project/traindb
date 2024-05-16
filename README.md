[![Java CI with Maven](https://github.com/traindb-project/traindb/actions/workflows/maven.yml/badge.svg)](https://github.com/traindb-project/traindb/actions/workflows/maven.yml)
[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/traindb-project/traindb/blob/main/examples/traindb_tutorial.ipynb)

# <img src="traindb-project/images/traindb_logo.png" alt="TrainDB" width="200" />

TrainDB is an ML model-based approximate query processing engine that aims to answer time-consuming analytical queries in a few seconds.
TrainDB will provide SQL-like query interface and support various DBMS data sources.

[Docs(English)](https://traindb-doc.readthedocs.io/en/latest/) • [Docs(Korean)](https://traindb-doc.readthedocs.io/ko/latest/) • [Tutorial(Colab)](https://colab.research.google.com/github/traindb-project/traindb/blob/main/examples/traindb_tutorial.ipynb)

## Requirements

* Java 11+
* Maven 3.x
* SQLite3 (or other DBMS for catalog store, supported by datanucleus)

For python environment setup, see README in our [traindb-model](https://github.com/traindb-project/traindb-model) repository.

## Install

### Download

```console
$ git clone --recurse-submodules https://github.com/traindb-project/traindb.git
```

### Build

```console
$ cd traindb
$ mvn package
```

Then, you can find traindb-x.y-SNAPSHOT.tar.gz in traindb-assembly/target directory.

```console
$ tar xvfz traindb-assembly/target/traindb-x.y-SNAPSHOT.tar.gz
```

## Run

### Example

Now, you can execute SQL statements using the command line interface.\
You need to put JDBC driver for your DBMS into the directory included in CLASSPATH.

```console
$ cd traindb-assembly/target/traindb-x.y-SNAPSHOT
$ bin/trsql
sqlline> !connect jdbc:traindb:<dbms>://<host>
Enter username for jdbc:traindb:<dbms>://localhost: <username> 
Enter password for jdbc:traindb:<dbms>://localhost: <password>
0: jdbc:traindb:<dbms>://<host>>
```

You can train ML models and run approximate queries like the following example.
```
0: jdbc:traindb:<dbms>://<host>> CREATE MODELTYPE tablegan FOR SYNOPSIS AS LOCAL CLASS 'TableGAN' IN '$TRAINDB_PREFIX/models/TableGAN.py';
No rows affected (0.255 seconds)
0: jdbc:traindb:<dbms>://<host>> TRAIN MODEL tgan MODELTYPE tablegan ON <schema>.<table>(<column 1>, <column 2>, ...);
epoch 1 step 50 tensor(1.1035, grad_fn=<SubBackward0>) tensor(0.7770, grad_fn=<NegBackward>) None
epoch 1 step 100 tensor(0.8791, grad_fn=<SubBackward0>) tensor(0.9682, grad_fn=<NegBackward>) None
...
0: jdbc:traindb:<dbms>://<host>> CREATE SYNOPSIS <synopsis> FROM MODEL tgan LIMIT <# of rows to generate>;
...
0: jdbc:traindb:<dbms>://<host>> SELECT APPROXIMATE avg(<column>) FROM <schema>.<table>;
```
