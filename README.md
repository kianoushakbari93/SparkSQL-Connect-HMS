**Spark 3 connection to Hive**

**Overview**
This script is designed to connect Spark to HMS(Hive metastore) to help with creating Delta tables and query them in a
better way. Delta Lake Table Creator Scripts will create a group of Delta tables inside HMS but in case if we want to
add more data or alter the table's property, only Spark support these operations on Delta tables.

**About Apache Spark**

Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala,
Python and R, and an optimised engine that supports general execution graphs. It also supports a rich set of
higher-level tools including Spark SQL for SQL and structured data processing, pandas API on Spark for pandas workloads,
MLlib for machine learning, GraphX for graph processing, and Structured Streaming for incremental computation and stream
processing.

**About script**

*The script process:*

- Download Apache Spark
- Download a few JAR libraries from Maven and put them inside the /jars directory for connecting the Spark to Hive
- Download delta-hive-assembly_2.13-3.2.0.jar and put it in /opt/hive-aux-jars path if doesn't exist on the current VM
- Copy the Hadoop site XML files to the Spark /conf directory
- Create the Delta warehouse directory in HDFS and give it Hive ownership
- At the end will run the Spark SQL with specified configs

*Spark specified configs in script set to connect to Hive*

- io.delta:delta-spark_2.13:3.2.0: Delta package 3.2.0 to run with Scala 2.13
- Set the spark.sql.extensions to io.delta.sql.DeltaSparkSessionExtension: For the Spark SQL to access the Delta library
  functions
- Set the spark.sql.catalog.spark_catalog to org.apache.spark.sql.delta.catalog.DeltaCatalog: For the built-in Spark
  catalog to be set default to Delta catalog
- Set the spark.sql.catalogImplementation to Hive: Hive support enabled, Spark can use the script transform with both
  Hive SerDe and ROW FORMAT DELIMITED
- Set the spark.kerberos.keytab & spark.kerberos.principal to use Hive keytab and principal
- Set the spark.hadoop.hive.metastore.uris to the Hive metastore thrift URI: To connect to metastore via thrift
- Set the spark.sql.hive.metastore.version to the version of Hive installed on the cluster
- Set the spark.sql.hive.metastore.jars to maven: Download all the JAR files for that specific version of Hive from
  Maven repository to run the Hive on Spark
- Set the spark.sql.warehouse.dir to the path on HDFS: This path is going to be the warehouse for storing the Delta data
- Set the spark.driver.memory to 2G: Assigning the amount of memory that Spark allowed to use

*Configurable parameters*

- HIVE_METASTORE_VERSION: The version of Hive on cluster. HINT: Run the "hive --version" on any VM
- HIVE_METASTORE_URIS: The thrift URI for HMS(Hive metastore). Search hive.metastore.uris in the hive-site.xml ->
  Usually HMS thrift is on vm0 on port 9083. Example: thrift://hms-server.com:9083
- HIVE_PRINCIPAL: The principal inside the Hive keytab
- HIVE_KEYTAB: Path to Hive keytab
- HDFS_PRINCIPAL: Principal inside the HDFS keytab
- HDFS_KEYTAB: Path to the HDFS keytab
- SPARK_DL_LINK: Link to download Apache Spark. Apache keep releasing the new versions and replacing them with the old
  ones. This means every time they release a new branch, the old link will become invalid. You can find latest release
  download link here https://spark.apache.org/downloads.html
- DELTA_WAREHOUSE_DIR: Define a valid path to store the Delta files on HDFS (check the namenode nameservice on your
  cluster)

**Pre-configurations on Cluster**

- On HMS VM run the commands bellow, to download the delta-hive-assembly_2.13-3.2.0.jar and put it in this path →
  /opt/hive-aux-jars/
  
- wget -P
  /opt/hive-aux-jars/ https://github.com/delta-io/delta/releases/download/v3.1.0/delta-hive-assembly_2.13-3.2.0.jar
  
- chown -R hive:hive /opt/hive-aux-jars/

*On HDP:*

- Add these properties to the Custom hive-site in the cluster manager to Hive: Cluster manager → Hive → ADVANCED →
  Custom hive-site
  
  - hive.aux.jars.path=file:///opt/hive-aux-jars/delta-hive-assembly_2.13-3.2.0.jar,<
    comma-separate-if-you-have-any-other-jar.jar>
  
  - hive.input.format=io.delta.hive.HiveInputFormat
 
  - hive.security.authorization.sqlstd.confwhitelist.append=hive\.input\.format|<pipe-will-separate-the-values>

- Search and change the hive.tez.input.format to io.delta.hive.HiveInputFormat inside the Hive: Cluster manager → Hive →
  ADVANCED → Advanced hive-site

- Search and find "Auxillary JAR list" in the Hive and set the value to the Delta uber jar file path: Cluster manager →
  Hive → ADVANCED

- Restart the required services to take effect.

*On CDP:*

- Add these 4 properties to the hive-site.xml through the hive-on-tez1 in the cluster manager. Cluster manager →
  hive-on-tez1 → Configuration → search for "Hive Service Advanced Configuration Snippet (Safety Valve) for
  hive-site.xml"
  
  - hive.security.authorization.sqlstd.confwhitelist.append=hive\.input\.format
  - hive.tez.input.format=io.delta.hive.HiveInputFormat
  - hive.input.format=io.delta.hive.HiveInputFormat
  - hive.aux.jars.path=file:///opt/hive-aux-jars/delta-hive-assembly_2.13-3.1.0.jar

- Search for Hive Auxiliary JARs Directory in the hive-on-tez1 in the cluster and set the value to the path for
  directory that contain the Delta uber JAR file: Cluster manager → hive-on-tez1 → Configuration → search for "Hive
  Auxiliary JARs Directory" and add this path → "/opt/hive-aux-jars/"

- Restart the required services to take effect.

**How to run the script**

- Become sudo (root) user
  su

- Make start-spark.sh script executable
  chmod +x start-spark.sh

- Run start script
  ./start-spark.sh

**Delta tables on Spark SQL**

- Creating Delta tables: command below will create and insert into the Delta table in the warehouse directory on HDFS.
  CREATE DATABASE dbx; USE dbx;
  CREATE EXTERNAL TABLE <table-name> (<column-identifier> <column-type>, ...) USING DELTA;
  INSERT INTO <table-name> VALUES (x1,y1,z1),(x2,y2,z2),...;
- This will create the Delta files in the HDFS and also creates a table on Hive. But there is a known
  issue(https://github.com/delta-io/delta/issues/1045) in Delta project at the moment that Spark will use the wrong
  table properties in Hive when creating the table "USING DELTA". This put the wrong serde in the table and also ignores
  creating the columns which Hive can't read the table correctly.

- To solve this issue, we need to drop the external table in the beeline and recreate it with the correct columns and
  storage handler that Hive can use in io.delta.

**IMPORTANT EXTRA STEP IF USING CDP:**

- On CDP, an external Delta table that is created using Spark will be TRANSLATED_TO_EXTERNAL on Hive and for that reason
  by default the table property will have external.table.purge=TRUE. It means after dropping the external table, will
  delete the table's data as well. To avoid this to happen, we need to alter the table to disable this:
  ALTER TABLE <table-name> SET TBLPROPERTIES("external.table.purge"="false");

- To create the correct Delta table in Hive after dropping the wrong one:
  CREATE EXTERNAL TABLE <table-name> (<column-identifier> <column-type>, ...)
  STORED BY 'io.delta.hive.DeltaStorageHandler'
  LOCATION '/delta/dbx.db/<table-name>'
  TBLPROPERTIES('DO_NOT_UPDATE_STATS'='true');
- This will create the Delta table with the correct properties that is readable on both Hive and Spark SQL. Now we can
  insert more data to the table on Spark SQL.

**Issues that still exist**

- The Delta table reader version that is created to be compatible with Hive is too old(between 1,2) and we can't rename
  the columns. https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-alter-table.html#rename-column

- Renaming the table is not possible because of Spark uses the Hive implementation on ALTERing the Delta table which
  throws "No such method exception". This need to be developed from Delta io project in their UBER JAR library:
  java.lang.NoSuchMethodException: org.apache.hadoop.hive.ql.metadata.Hive.alterTable(java.lang.String,
  org.apache.hadoop.hive.ql.metadata.Table, org.apache.hadoop.hive.metastore.api.EnvironmentContext)

- To have the ability to rename a column, we need to use the newer Delta table reader version properties. This is
  possible to do by changing the Delta reader version and set the column mapping to name, but the issue is that Hive
  won't be able to read this Delta table anymore. The DELTA UBER JAR that is used. to read the Delta files uses the
  older version of Delta reader. An example to set these properties and rename the columns in Spark
  SQL: https://docs.databricks.com/en/delta/delta-column-mapping.html#how-to-enable-delta-lake-column-mapping
  ALTER TABLE table3 SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','
  delta.minWriterVersion' = '5')
  ALTER TABLE <table-name> RENAME COLUMN old_col_name TO new_col_name

- To add a column to the existing table is possible by ALTER command in Spark, but the Hive have problem with reading
  the table after adding the column:
  ALTER TABLE <table-name> ADD COLUMNS (<column-identifier> <column-type>);
- In the Hive though when trying to query it:
  Error: Error while compiling statement: FAILED: SemanticException The Delta table schema is not the same as the Hive
  schema:Specified schema is missing field(s): <column-identifier>Delta table schema:
  root
  -- <column1-identifier>: <column1-type> (nullable = true) (metadata ={})
  -- <column2-identifier>: <column2-type> (nullable = true) (metadata ={})
  -- <column3-identifier>: <column3-type> (nullable = true) (metadata ={})
  Hive schema:
  <column1-identifier>: <column1-type>
  <column2-identifier>: <column2-type>Please update your Hive table's schema to match the Delta table schema. (
  state=42000,code=40000)

- Creating Delta tables with partitions in Spark SQL, is not showing/supported in Hive. To query this table in Hive and
  still have the ability to insert into it in Spark SQL create the table in Spark:
  CREATE EXTERNAL TABLE <tablle-name> (<column-identifier> <column-type>, ...) USING DELTA PARTITIONED BY (<
  part-col-name> <part-col-type>);
- In Hive, drop the table and recreate it with the partitioned columns:
  ALTER TABLE <table-name> SET TBLPROPERTIES("external.table.purge"="false")
  DROP TABLE <table-name>;
  CREATE EXTERNAL TABLE <table-name> (<column-identifier> <column-type>, ..., <part-col-name> <part-col-type>) STORED
  BY 'io.delta.hive.DeltaStorageHandler' LOCATION '/delta/table/path' TBLPROPERTIES('DO_NOT_UPDATE_STATS'='true');
