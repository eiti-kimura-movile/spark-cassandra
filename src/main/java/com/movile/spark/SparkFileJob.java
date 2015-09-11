package com.movile.spark;

import java.io.File;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.google.common.base.Stopwatch;

public class SparkFileJob {

    public static void main(String[] args) {
        
        // inform your spark home here
        String SPARK_HOME = "/opt/spark-1.4.0-bin-hadoop2.6";

        JavaSparkContext sc = new JavaSparkContext("local[*]", "Simple App", SPARK_HOME, "./build/libs/spark-simple-1.0.jar");
        Stopwatch chrono = Stopwatch.createStarted();
        
        // Get file from resources folder
        ClassLoader classLoader = SparkFileJob.class.getClassLoader();
        File file = new File(classLoader.getResource("plain-dataset-10k.json").getFile());

        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().json(file.getPath());
        df.registerTempTable("subscriptions");

        String query  = "SELECT phone, MAX(charge_priority) as max_priority, MAX(user_plan) as max_plan, MIN(last_renew_attempt) AS min_last_renew_attempt FROM subscriptions ";
        query += "WHERE enabled = 1 ";
        query += "AND timeout_date < " + System.currentTimeMillis() + " ";
        query += "AND related_id IS NULL ";
        query += "AND carrier_id in (1, 4, 2, 5) ";
        query += "GROUP BY phone ";
        query += "ORDER BY max_priority DESC, max_plan DESC, min_last_renew_attempt";

        sqlContext.sql(query)
           .javaRDD()
           .foreach(row -> process(row));

        chrono.stop();
        sc.close();
        sc.stop();
    }

    private static void process(Row row) {
       System.out.println("Processing row: " + row);
    }

}
