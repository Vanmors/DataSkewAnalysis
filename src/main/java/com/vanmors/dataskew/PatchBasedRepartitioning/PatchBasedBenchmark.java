package com.vanmors.dataskew.PatchBasedRepartitioning;

import com.vanmors.dataskew.BenchmarkResult;
import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Бенчмарк Patch-based Repartitioning.
 * На основе: Kassela E. (2023–2025).
 *
 * Алгоритм:
 *   1) Определяем скошенные ключи (частота > SKEW_THRESHOLD * total).
 *   2) Для каждого скошенного ключа вычисляем число патчей: ceil(count / PATCH_SIZE).
 *   3) Каждой строке скошенного ключа назначаем patch_id через rand()
 *      (равномерное случайное распределение по [0, num_patches)).
 *      — Это заменяет row_number() по окну, который собрал бы все 7M строк
 *        в одну партицию и вызвал OOM/выплёскивание на диск.
 *   4) Реплицируем строки users для скошенных ключей по всем patch_id (explode sequence).
 *   5) Обычным строкам и их users назначаем patch_id = 0.
 *   6) Join по составному ключу (user_id, patch_id).
 */
public class PatchBasedBenchmark {

    private static final double SKEW_THRESHOLD = 0.01;
    private static final int PATCH_SIZE = 50_000;

    public static BenchmarkResult run(final SparkSession spark,
                                      final Dataset<Row> transactions,
                                      final Dataset<Row> users,
                                      final String skewLevel,
                                      final double skewFraction) {
        final long totalTransactions = transactions.count();

        return BenchmarkUtils.measure(spark, "Patch-based", skewLevel, skewFraction, () -> {
            final long threshold = (long) (totalTransactions * SKEW_THRESHOLD);

            final Dataset<Row> keyCounts = transactions
                    .groupBy("user_id")
                    .agg(functions.count("*").alias("cnt"))
                    .filter(functions.col("cnt").gt(threshold))
                    .withColumn("num_patches",
                            functions.ceil(
                                    functions.col("cnt").cast(DataTypes.DoubleType).divide(PATCH_SIZE)
                            ).cast(DataTypes.IntegerType));
            keyCounts.cache().count();

            final Dataset<Row> skewedKeys = keyCounts.select("user_id");

            // Разделяем transactions на скошенные и обычные
            final Dataset<Row> txSkewed = transactions.join(functions.broadcast(skewedKeys), "user_id");
            final Dataset<Row> txNormal = transactions.join(
                    functions.broadcast(skewedKeys),
                    transactions.col("user_id").equalTo(skewedKeys.col("user_id")),
                    "left_anti");

            // Patch-id для скошенных: rand() % num_patches — без window function
            final Dataset<Row> txSkewedPatched = txSkewed
                    .join(functions.broadcast(keyCounts.select("user_id", "num_patches")), "user_id")
                    .withColumn("patch_id",
                            functions.floor(functions.rand().multiply(functions.col("num_patches")))
                                    .cast(DataTypes.IntegerType))
                    .drop("num_patches");

            final Dataset<Row> txNormalPatched = txNormal.withColumn("patch_id", functions.lit(0));
            final Dataset<Row> allTransactions = txSkewedPatched.unionByName(txNormalPatched);

            // Реплицируем users для скошенных ключей по всем patch_id
            final Dataset<Row> skewedUsersExpanded = users
                    .join(functions.broadcast(keyCounts.select("user_id", "num_patches")), "user_id")
                    .withColumn("patch_id",
                            functions.explode(functions.sequence(
                                    functions.lit(0),
                                    functions.col("num_patches").minus(1))))
                    .drop("num_patches");

            // Обычные users получают patch_id = 0
            final Dataset<Row> normalUsers = users
                    .join(functions.broadcast(skewedKeys),
                            users.col("user_id").equalTo(skewedKeys.col("user_id")),
                            "left_anti")
                    .withColumn("patch_id", functions.lit(0));

            final Dataset<Row> allUsers = skewedUsersExpanded.unionByName(normalUsers);

            final Dataset<Row> result = allTransactions.join(allUsers,
                    allTransactions.col("user_id").equalTo(allUsers.col("user_id"))
                            .and(allTransactions.col("patch_id").equalTo(allUsers.col("patch_id"))));

            keyCounts.unpersist();
            return result.count();
        });
    }
}
