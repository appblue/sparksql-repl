# sparksql-repl
REPL for running SparkSQL

## Example of using CSV file

It's possible to create table from CSV file, that is avaialable in the REPL

```
$ ./sparkrepl.py

>>> create table names (id INT, name STRING) USING CSV LOCATION 'file:/Users/kkielak/_work_/sparksql-repl/_data_/t1.csv';
++
||
++
++

>>>
... select * from names;
+---+----+
| id|name|
+---+----+
| 10|maja|
|  5|nina|
|  5|maks|
|  1|kris|
+---+----+
```

The contents of the file is following:

```shell
cat _data_/t1.csv
10,maja
5,nina
5,maks
1,kris
```

