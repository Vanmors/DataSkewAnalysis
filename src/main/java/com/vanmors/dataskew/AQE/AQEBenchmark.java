package com.vanmors.dataskew.AQE;

import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Бенчмарк Adaptive Query Execution (AQE).
 *
 * Запускает join двух таблиц (transactions ⨝ users по user_id) дважды:
 *   1) AQE выключен (базовый замер)
 *   2) AQE включён (с оптимизацией skew join)
 */
public class AQEBenchmark {

    public static void main(String[] args) {
        SparkSession spark = BenchmarkUtils.createSparkSession("AQE Benchmark");

        Dataset<Row>[] data = BenchmarkUtils.generateAndCache(spark);
        Dataset<Row> transactions = data[0];
        Dataset<Row> users = data[1];

        // --- Базовый запуск (AQE OFF) ---
        System.out.println(">>> Запуск JOIN без AQE <<<");
        BenchmarkUtils.measureAndPrint(spark, "AQE OFF", () ->
                transactions.join(users, "user_id").count());

        // --- Запуск с AQE ---
        System.out.println(">>> Запуск JOIN с AQE <<<");
        spark.conf().set("spark.sql.adaptive.enabled", "true");
        spark.conf().set("spark.sql.adaptive.skewJoin.enabled", "true");
        spark.conf().set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5");
        spark.conf().set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb");
        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "true");

        BenchmarkUtils.measureAndPrint(spark, "AQE ON", () ->
                transactions.join(users, "user_id").count());

        // Возвращаем AQE в исходное состояние
        spark.conf().set("spark.sql.adaptive.enabled", "false");

        spark.stop();
    }
}
