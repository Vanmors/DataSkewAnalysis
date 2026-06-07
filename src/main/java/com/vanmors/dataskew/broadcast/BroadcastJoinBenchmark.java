package com.vanmors.dataskew.broadcast;

import com.vanmors.dataskew.BenchmarkResult;
import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Бенчмарк Broadcast Join.
 *
 * Таблица users целиком рассылается на все исполнители через broadcast hint.
 * Join выполняется локально без shuffle, что полностью устраняет перекос
 * при условии, что users помещается в память каждого исполнителя.
 */
public class BroadcastJoinBenchmark {

    public static BenchmarkResult run(final SparkSession spark,
                                      final Dataset<Row> transactions,
                                      final Dataset<Row> users,
                                      final String skewLevel,
                                      final double skewFraction) {
        return BenchmarkUtils.measure(spark, "Broadcast Join", skewLevel, skewFraction,
                () -> transactions.join(functions.broadcast(users), "user_id").count());
    }
}
