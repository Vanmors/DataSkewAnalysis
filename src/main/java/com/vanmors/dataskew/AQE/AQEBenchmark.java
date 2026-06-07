package com.vanmors.dataskew.AQE;

import com.vanmors.dataskew.BenchmarkResult;
import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Бенчмарк Adaptive Query Execution (AQE).
 *
 * Включает AQE со skew join оптимизацией. Spark автоматически обнаруживает
 * скошенные партиции и разбивает их на подзадачи.
 */
public class AQEBenchmark {

    public static BenchmarkResult run(final SparkSession spark,
                                      final Dataset<Row> transactions,
                                      final Dataset<Row> users,
                                      final String skewLevel,
                                      final double skewFraction) {
        spark.conf().set("spark.sql.adaptive.enabled", "true");
        spark.conf().set("spark.sql.adaptive.skewJoin.enabled", "true");
        spark.conf().set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5");
        spark.conf().set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb");
        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "true");

        final BenchmarkResult result = BenchmarkUtils.measure(spark, "AQE", skewLevel, skewFraction,
                () -> transactions.join(users, "user_id").count());

        spark.conf().set("spark.sql.adaptive.enabled", "false");
        spark.conf().set("spark.sql.adaptive.skewJoin.enabled", "false");
        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "false");

        return result;
    }
}
