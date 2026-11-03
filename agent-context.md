# Using Sail (agent context)

Sail runs most Spark SQL and DataFrame code unchanged over Spark Connect. Write ordinary Spark
code; the points below are where Sail needs specific knowledge. Each point is backed by an
automated check against pysail==0.7.3 with pyspark-client==4.1.3; details attributed to the Sail docs or marked
as measured separately are not covered by that check.

## Run and connect

- Connect to Sail with a normal PySpark Spark Connect session. In a script, start Sail in-process with `pysail.spark.SparkConnectServer()`, call `start()`, read the port from `server.listening_address`, and connect with `SparkSession.builder.remote(f"sc://localhost:{port}")`. As a long-running service, run `sail spark server` (default `127.0.0.1:50051`, per the Sail docs). `SELECT version()` returns the Sail version; `spark.version` returns the PySpark client's version.

## Configure

- Set Sail engine options as `SAIL_<SECTION>__<KEY>` environment variables before the server starts, for example `SAIL_EXECUTION__DEFAULT_PARALLELISM=8`. `spark.conf.set` changes only the Spark SQL session options Sail maps, such as `spark.sql.session.timeZone`; `spark.sql.shuffle.partitions` has no effect.
- Configure catalogs with the `SAIL_CATALOG__LIST` environment variable (a TOML array) and `SAIL_CATALOG__DEFAULT_CATALOG` before the server starts; the Sail catalog docs cover Iceberg REST, Unity, Glue and Hive Metastore entries. JVM-style `spark.sql.catalog.<name>` settings do not create catalogs.
- With no catalog configured, the current catalog is named `sail`, not `spark_catalog`. Avoid hard-coding `spark_catalog` in three-part table names.

## Python UDFs

- In local mode (the default), Python UDFs run inside the Sail server process, in one Python interpreter shared by all tasks, while Apache Spark runs them in separate Python worker processes. Prefer built-in functions, and for CPU-heavy Python work prefer libraries that release the GIL (NumPy, PyArrow compute, ONNX Runtime, tokenizers) so tasks can overlap.
- In local mode, Sail runs the partitions of a Python UDF at the same time as threads of the server process (Apache Spark uses separate Python worker processes). A library that must not be called from two threads at once (PDFium through pypdfium2, per its docs) therefore needs one lock for the whole process. Keep that lock in `sys.modules`, which every task and every later query in the process share. A global of your script will not do: its functions are pickled by value, so each task gets its own copy of the globals.
- To load a model or client once per partition, put the setup before the loop in `mapInArrow`, `mapInPandas` or an iterator `pandas_udf`. In an iterator `arrow_udf`, code before the loop runs once per batch instead.
- `spark.addArtifacts()` is not supported, so Python files and packages cannot be shipped from the client at run time. Install every library your UDFs import into the Python environment the Sail server runs in.

## Data sources

- To process loose files (images, PDFs, audio, documents), build a DataFrame of file paths and read each file inside `mapInArrow` or a pandas UDF, returning only what later stages need (text, features, embeddings). This gives exactly one row per file and keeps large binaries out of the data passed between stages. Paths must be readable from where the UDF runs (local files on one machine, object storage on a cluster).
- Parquet, CSV, JSON, text and Delta Lake read and write with the standard `spark.read` and `df.write` calls and need no extra packages. Avro can be read but not written. ORC (read or write) and the `image` source are not available.
- Inferred schemas differ from Apache Spark's in three ways: CSV `inferSchema` reads whole numbers as `BIGINT` (Spark: `INT`); JSON keeps columns in file order (Spark sorts them by name); and partition columns of a partitioned directory come back as strings (Spark infers numbers and dates). Pass an explicit schema (`spark.read.schema(...)`) when types or column order matter.

## Not available

- `pyspark.ml` (MLlib) is not available, although Spark Connect 4.x supports it. Train and score models with Python libraries (scikit-learn, PyTorch, ONNX Runtime) inside `mapInArrow` or a pandas UDF, or outside the engine.
- `spark.sparkContext` and the RDD API are not available, as on any Spark Connect server. Use the DataFrame and SQL APIs instead, for example `spark.createDataFrame([], schema)` rather than `sparkContext.emptyRDD()`.

## Behaves differently from Spark

- Sail does not support dynamic partition overwrite for Parquet: with `partitionOverwriteMode` set to `dynamic` (in the session or as a writer option), `df.write.mode("overwrite").partitionBy(c)` still replaces every partition of the path, and `INSERT OVERWRITE` replaces the whole table, where Apache Spark replaces only the partitions the data contains. Delta Lake tables follow the setting on both engines. `INSERT OVERWRITE t PARTITION (c = value)` is not supported. To replace one Parquet partition, overwrite its own directory (`path/c=value`, without the partition column), or write Delta Lake.
- In regular expressions (`regexp_replace`, `regexp_extract`, `rlike`, `split`), Sail's `\s`, `\d` and `\w` match Unicode characters (an ideographic space, an Arabic-Indic digit, `é`), while Apache Spark's match ASCII only, so the same pattern can return different results on non-ASCII text. For the same results on both, write the class you mean: `[ \t\n\r]`, `[0-9]` and `[A-Za-z0-9_]` for ASCII, or `[\p{Z}\t\n\r]`, `\p{Nd}` and `[\p{L}\p{Nd}_]` for Unicode. The `(?U)` flag is not a portable switch: Spark reads it as Unicode classes, Sail as lazy quantifiers.
- Dividing DECIMAL values gives a different result type on Sail than on Apache Spark (sometimes fewer, sometimes more decimal places), and Sail truncates where Spark rounds. `7 / 60.0` is 0.1166 on Sail and 0.116667 on Spark; `CAST(2 AS DECIMAL(10,0)) / CAST(3 AS DECIMAL(10,0))` is 0.6666 and 0.66666666667. A literal with a decimal point, such as `60.0`, is a DECIMAL. For the same results on both, divide DOUBLE values (`x / 60.0D`, `CAST(x AS DOUBLE) / y`) or integers (`x / 60`, which returns a DOUBLE on both), and round explicitly when you need a fixed number of digits.
- `cache()` and `persist()` are accepted but do not keep results: every action recomputes the DataFrame, including any UDF or model call in it. To reuse an expensive result (for example model outputs), write it to Parquet or a table and read it back.

## AI and vector functions

- For embeddings, store vectors as `ARRAY<FLOAT>` and use `vector_cosine_similarity`, `vector_inner_product` and `vector_l2_distance` (Spark 4.2 has them too). Both engines reject `ARRAY<DOUBLE>`; Sail also rejects Arrow fixed-size lists (as written by NumPy or PyArrow), which Spark accepts. `CAST(col AS ARRAY<FLOAT>)` first.

20 more checked facts are in llms.txt.

Docs: https://docs.lakesail.com/sail/latest/
Check Spark code for unsupported features: https://migrate.lakesail.com/
