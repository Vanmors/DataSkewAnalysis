package com.vanmors.dataskew.preaggregation;

import com.vanmors.dataskew.BenchmarkResult;
import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Бенчмарк Pre-aggregation (map-side reduce).
 *
 * Перед join-ом transactions агрегируются по user_id: count, sum, avg.
 * Результат — ~NUM_USERS строк вместо 10M, горячий ключ сворачивается в одну строку.
 * Join выполняется на значительно меньшем объёме данных.
 *
 * Ограничение: метод применим только когда семантика запроса допускает предварительную
 * агрегацию (нельзя получить отдельные транзакции после агрегации).
 */
public class PreAggregationBenchmark {

    public static BenchmarkResult run(final SparkSession spark,
                                      final Dataset<Row> transactions,
                                      final Dataset<Row> users,
                                      final String skewLevel,
                                      final double skewFraction) {
        return BenchmarkUtils.measure(spark, "Pre-aggregation", skewLevel, skewFraction, () -> {
            final Dataset<Row> aggregated = transactions
                    .groupBy("user_id")
                    .agg(
                            functions.count("transaction_id").alias("tx_count"),
                            functions.sum("amount").alias("total_amount"),
                            functions.avg("amount").alias("avg_amount")
                    );
            return aggregated.join(users, "user_id").count();
        });
    }
}
