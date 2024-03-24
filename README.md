### spark hbase bulkload
* spark3.x hbase2.x (EMR-6.10.1+)
#### build
```shell
mvn clean package -Dscope.type=provided
# direct download
```
#### test data
```shell
# test data
S3_BUCKET="s3://panchao-data"
cat <<EOF -> test.json
{"key":"123","col1":"customer","col2":"aws"}
EOF

aws s3 cp test.json $S3_BUCKET/hbase-data/
```
#### job params
```shell

HBaseBulkLoad 1.0
Usage: spark HBaseBulkLoad [options]

  -e, --env <value>        env: dev or prod
  -z, --hbaseZK <value>    hbaseZK,default: localhost:2181
  -s, --sourceDir <value>  sourceDir,source data
  -t, --targetDir <value>  targetDir,dir for save hfile
  -n, --tableName <value>  tableName,hbase table name
  -k, --rowKey <value>     row key column
  -c, --cf <value>         column family name
  -m, --columns <value>    columns name, eg: col1,col2
  -p, --namespace <value>  hbase namespace,default: default

```
#### spark submit job
```shell
S3_BUCKET="s3://panchao-data"
spark-submit \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1024M \
--conf spark.driver.memory=1024M \
--conf spark.executor.instances=2 \
--conf spark.dynamicAllocation.enabled=true \
--class com.aws.analytics.HBaseBulkLoad \
${S3_BUCKET}/spark-hbase-bulkload-1.0-jar-with-dependencies.jar  \
-z 10.1.142.214:2181 \
-s ${S3_BUCKET}/hbase-data/ \
-t ${S3_BUCKET}/hbase-hfile/105/ \
-n usertable \
-k key \
-m col2,col1 \
-c cf \

```

#### load hfile
```shell
sudo -u hbase hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles s3://panchao-data/hbase-hfile/105/ usertable
```