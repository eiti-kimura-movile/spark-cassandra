package com.movile.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

public class SparkCassandraJobMain {

    public static void main(String[] args) {

        // http://www.datastax.com/dev/blog/common-spark-troubleshooting
        String[] jars = {
                "./libs/cassandra-driver-core-2.1.7.1.jar",
                "./libs/spark-cassandra-connector_2.10-1.4.0-M3.jar",
                "./libs/guava-18.0.jar",
                "./libs/joda-time-2.8.2.jar"
        };
        
        SparkConf conf = new SparkConf(true)
        .setAppName("movile-index")
        .setMaster("spark://127.0.0.1:7077")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .setJars(jars);
        
        JavaSparkContext sc = new JavaSparkContext(conf);

        CassandraSQLContext cassandraContext = new CassandraSQLContext(sc.sc());
        DataFrame df = cassandraContext.sql("SELECT conf_id, count(1) FROM "
                                                  + "movile.idx_config_ref GROUP BY conf_id "
                                                  + "ORDER BY conf_id");
        
        //process data
        df.javaRDD()
          .foreach(row -> process(row));
        
        sc.close();
        sc.stop();
    }

    private static void process(Row row) {
        System.out.println("data processed: " + row);
    }
    
}
