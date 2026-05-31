package com.vanmors.dataskew.preaggregation;

import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Бенчмарк Pre-aggregation (map-side reduce).
 *
 * Идея: перед join-ом предварительно агрегировать большую (скошенную)
 * таблицу по ключу join. Это уменьшает количество строк с горячим ключом
 * до одной агрегированной строки, устраняя перекос.
 *
 * После агрегации join выполняется на значительно меньшем объёме данных.
 *
 * Сравнение: обычный join vs. join с pre-aggregation.
 */
public class PreAggregationBenchmark {

    public static void main(String[] args) {
        SparkSession spark = BenchmarkUtils.createSparkSession("Pre-aggregation Benchmark");

        Dataset<Row>[] data = BenchmarkUtils.generateAndCache(spark);
        Dataset<Row> transactions = data[0];
        Dataset<Row> users = data[1];

        // --- Обычный join ---
        System.out.println(">>> Обычный JOIN <<<");
        BenchmarkUtils.measureAndPrint(spark, "Без pre-aggregation", () ->
                transactions.join(users, "user_id").count());

        // --- Pre-aggregation + join ---
        System.out.println(">>> JOIN с pre-aggregation <<<");
        BenchmarkUtils.measureAndPrint(spark, "Pre-aggregation", () -> {
            // Агрегируем transactions по user_id
            Dataset<Row> aggregated = transactions
                    .groupBy("user_id")
                    .agg(
                            functions.count("transaction_id").alias("tx_count"),
                            functions.sum("amount").alias("total_amount"),
                            functions.avg("amount").alias("avg_amount")
                    );

            // Join агрегированных данных с users
            return aggregated.join(users, "user_id").count();
        });

        spark.stop();
    }
}
