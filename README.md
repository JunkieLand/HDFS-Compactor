# HDFS Compactor

This is a Work In Progress.

## Motivation

Handling small files are a common issue with HDFS. When a job with Apache Spark for instance will ofter result with many small files for each partition will write it's own file. However, small files will take a lot of Namenode resources. Besides, HDFS being optimized for streaming large chunck of data, many small files will have a very negative impact on IO performance.

The purpose of the HDFS Compactor is to concatenate small files into bigger files.

## Supported file formats

### Parquet

Since Parquet is a splittable file format, it is possible to concatenate every small file into one big file, while still getting advantage of compression. In order to optimize IO when reading the big file latter, HDFS Compactor will set the Parquet block size to be the same size as an HDFS block.

### Avro

Avro being a splittable file as Parquet, the same applies here. HDFS Compactor will also set the Avro block size to be the same size as an HDFS block.

### CSV

CSV being a simple text file, it is not splittable. Thus there are two possible solutions :

* Use a splittable compression format like Bzip2 or LZO
* Use a non splittable compression format like GZip or Snappy, and write several files instead of one, but making their size closer to the HDFS block size

Though it is a splittable compression format, Bzip2 is out of the game because it's very CPU intensive.

LZO is the best solution, however because of its GPL licence being incompatible with the Apache licence of Hadoop, you have to install it yourself.

The default setting chosen for HDFS Compactor is then GZip.

## Alternatives

Some alternatives exist to compact specific file formats. However because of some limitations, the choice has been made in HDFS Compactor not to use them, and to rely on Spark instead.

### Parquet

[parquet-tools](https://github.com/apache/parquet-mr/tree/master/parquet-tools) has a `merge` command which concatenate parquet files into one. There are two limitations with this tool :

* files are simply concatenated, so the Parquet block size of the output file may not match the HDFS block size
* there is a limit to the number of file it can concatenate at once. The limit is hardcoded to `100`.

### Avro

[avro-tools](https://avro.apache.org/docs/1.8.1/gettingstartedjava.html)