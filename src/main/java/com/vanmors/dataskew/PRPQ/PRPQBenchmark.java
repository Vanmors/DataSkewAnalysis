package com.vanmors.dataskew.PRPQ;

import com.vanmors.dataskew.BenchmarkResult;
import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Бенчмарк алгоритма PRPQ (Partial Redistribution & Partial Query).
 * На основе: Cheng et al., 2014.
 *
 * Алгоритм:
 *   1) Определяем «тяжёлые» ключи (частота > HEAVY_KEY_THRESHOLD * total).
 *   2) Для тяжёлых ключей — broadcast join (нет shuffle).
 *   3) Для остальных ключей — обычный shuffle join.
 *   4) Объединяем результаты через unionAll.
 *
 * heavyKeys broadcast-ится явно при фильтрации, что избегает лишнего shuffle.
 */
public class PRPQBenchmark {

    private static final double HEAVY_KEY_THRESHOLD = 0.01;

    public static BenchmarkResult run(final SparkSession spark,
                                      final Dataset<Row> transactions,
                                      final Dataset<Row> users,
                                      final String skewLevel,
                                      final double skewFraction) {
        final long totalTransactions = transactions.count();

        return BenchmarkUtils.measure(spark, "PRPQ", skewLevel, skewFraction, () -> {
            final long threshold = (long) (totalTransactions * HEAVY_KEY_THRESHOLD);

            final Dataset<Row> heavyKeys = transactions
                    .groupBy("user_id")
                    .agg(functions.count("*").alias("cnt"))
                    .filter(functions.col("cnt").gt(threshold))
                    .select("user_id");
            heavyKeys.cache().count();

            // Разделяем transactions с broadcast heavyKeys — без shuffle при фильтрации
            final Dataset<Row> txHeavy = transactions.join(functions.broadcast(heavyKeys), "user_id");
            final Dataset<Row> txLight = transactions.join(
                    functions.broadcast(heavyKeys),
                    transactions.col("user_id").equalTo(heavyKeys.col("user_id")),
                    "left_anti");

            // Тяжёлые ключи: broadcast join с соответствующими строками users
            final Dataset<Row> usersForHeavy = users.join(functions.broadcast(heavyKeys), "user_id");
            final Dataset<Row> resultHeavy = txHeavy
                    .join(functions.broadcast(usersForHeavy), "user_id")
                    .select("user_id", "transaction_id", "amount", "name");

            // Лёгкие ключи: обычный shuffle join
            final Dataset<Row> resultLight = txLight
                    .join(users, "user_id")
                    .select("user_id", "transaction_id", "amount", "name");

            final long count = resultHeavy.unionAll(resultLight).count();
            heavyKeys.unpersist();
            return count;
        });
    }
}
