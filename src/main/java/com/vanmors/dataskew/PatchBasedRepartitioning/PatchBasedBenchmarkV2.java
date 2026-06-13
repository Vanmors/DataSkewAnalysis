package com.vanmors.dataskew.PatchBasedRepartitioning;

import com.vanmors.dataskew.BenchmarkResult;
import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Patch-based Repartitioning, вариант V2 с PATCH_SIZE = 500_000.
 *
 * Алгоритм идентичен оригинальному PatchBasedBenchmark; цель копии —
 * исследовать чувствительность метода к размеру патча на multi-skew данных,
 * не трогая baseline-реализацию.
 *
 * Для STRONG-распределения (3.5M строк на самом горячем ключе)
 * num_patches = 7, что близко к числу солей в SaltingBenchmark (10).
 */
public class PatchBasedBenchmarkV2 {

    private static final double SKEW_THRESHOLD = 0.01;
    private static final int PATCH_SIZE = 500_000;

    public static BenchmarkResult run(final SparkSession spark,
                                      final Dataset<Row> transactions,
                                      final Dataset<Row> users,
                                      final String skewLevel,
                                      final double skewFraction) {
        final long totalTransactions = transactions.count();

        return BenchmarkUtils.measure(spark, "Patch-based-500K", skewLevel, skewFraction, () -> {
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

            final Dataset<Row> txSkewed = transactions.join(functions.broadcast(skewedKeys), "user_id");
            final Dataset<Row> txNormal = transactions.join(
                    functions.broadcast(skewedKeys),
                    transactions.col("user_id").equalTo(skewedKeys.col("user_id")),
                    "left_anti");

            final Dataset<Row> txSkewedPatched = txSkewed
                    .join(functions.broadcast(keyCounts.select("user_id", "num_patches")), "user_id")
                    .withColumn("patch_id",
                            functions.floor(functions.rand().multiply(functions.col("num_patches")))
                                    .cast(DataTypes.IntegerType))
                    .drop("num_patches");

            final Dataset<Row> txNormalPatched = txNormal.withColumn("patch_id", functions.lit(0));
            final Dataset<Row> allTransactions = txSkewedPatched.unionByName(txNormalPatched);

            final Dataset<Row> skewedUsersExpanded = users
                    .join(functions.broadcast(keyCounts.select("user_id", "num_patches")), "user_id")
                    .withColumn("patch_id",
                            functions.explode(functions.sequence(
                                    functions.lit(0),
                                    functions.col("num_patches").minus(1))))
                    .drop("num_patches");

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
