package com.vanmors.dataskew.PatchBasedRepartitioning;

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
 * Идея алгоритма:
 *   1) Определить скошенные ключи путём анализа распределения.
 *   2) Для скошенных ключей: разбить их данные на «патчи» — подгруппы
 *      фиксированного размера, назначая каждой строке номер патча.
 *      Это превращает один скошенный ключ в множество составных ключей
 *      (user_id, patch_id), равномерно распределённых по партициям.
 *   3) Реплицировать соответствующие строки из малой таблицы
 *      по всем patch_id скошенного ключа.
 *   4) Для нескошенных ключей: назначить patch_id = 0.
 *   5) Выполнить join по составному ключу (user_id, patch_id).
 *
 * Отличие от salting: патчи определяются детерминированно на основе
 * анализа распределения, а не случайно; реплицируются только строки
 * для скошенных ключей, а не вся малая таблица.
 *
 * Сравнение: обычный join vs. patch-based join.
 */
public class PatchBasedBenchmark {

    // Порог для определения скошенного ключа (доля от общего числа строк)
    private static final double SKEW_THRESHOLD = 0.01;
    // Целевой размер патча (количество строк на патч)
    private static final int PATCH_SIZE = 50_000;

    public static void main(String[] args) {
        SparkSession spark = BenchmarkUtils.createSparkSession("Patch-based Repartitioning Benchmark");

        Dataset<Row>[] data = BenchmarkUtils.generateAndCache(spark);
        Dataset<Row> transactions = data[0];
        Dataset<Row> users = data[1];

        long totalTransactions = transactions.count();

        // --- Обычный join ---
        System.out.println(">>> Обычный JOIN <<<");
        BenchmarkUtils.measureAndPrint(spark, "Shuffle Join", () ->
                transactions.join(users, "user_id").count());

        // --- Patch-based join ---
        System.out.println(">>> Patch-based Repartitioning JOIN <<<");
        BenchmarkUtils.measureAndPrint(spark, "Patch-based", () -> {

            long threshold = (long) (totalTransactions * SKEW_THRESHOLD);

            // Шаг 1: определяем скошенные ключи и число патчей для каждого
            Dataset<Row> keyCounts = transactions
                    .groupBy("user_id")
                    .agg(functions.count("*").alias("cnt"))
                    .filter(functions.col("cnt").gt(threshold))
                    .withColumn("num_patches",
                            functions.ceil(functions.col("cnt").cast(DataTypes.DoubleType).divide(functions.lit(PATCH_SIZE)))
                                    .cast(DataTypes.IntegerType));

            keyCounts.cache().count();

            // Собираем только user_id скошенных ключей для фильтрации
            Dataset<Row> skewedKeys = keyCounts.select("user_id");

            // Шаг 2: разделяем transactions на скошенные и обычные
            Dataset<Row> txSkewed = transactions.join(skewedKeys, "user_id");
            Dataset<Row> txNormal = transactions.join(skewedKeys,
                    transactions.col("user_id").equalTo(skewedKeys.col("user_id")), "left_anti");

            // Назначаем patch_id скошенным строкам через row_number по окну
            var windowSpec = org.apache.spark.sql.expressions.Window
                    .partitionBy("user_id")
                    .orderBy("transaction_id");

            Dataset<Row> txSkewedPatched = txSkewed
                    .withColumn("patch_id",
                            functions.floor(
                                    functions.row_number().over(windowSpec).minus(1)
                                            .divide(functions.lit(PATCH_SIZE))
                            ).cast(DataTypes.IntegerType));

            // Обычным строкам назначаем patch_id = 0
            Dataset<Row> txNormalPatched = txNormal
                    .withColumn("patch_id", functions.lit(0));

            // Объединяем
            Dataset<Row> allTransactions = txSkewedPatched.unionByName(txNormalPatched);

            // Шаг 3: реплицируем users для скошенных ключей по patch_id
            Dataset<Row> skewedUsersExpanded = users
                    .join(keyCounts.select("user_id", "num_patches"), "user_id")
                    .withColumn("patch_id",
                            functions.explode(
                                    functions.sequence(functions.lit(0),
                                            functions.col("num_patches").minus(1))))
                    .drop("num_patches");

            // Для нескошенных ключей — patch_id = 0
            Dataset<Row> normalUsers = users.join(skewedKeys,
                            users.col("user_id").equalTo(skewedKeys.col("user_id")), "left_anti")
                    .withColumn("patch_id", functions.lit(0));

            Dataset<Row> allUsers = skewedUsersExpanded.unionByName(normalUsers);

            // Шаг 4: join по составному ключу (user_id, patch_id)
            Dataset<Row> result = allTransactions.join(allUsers,
                    allTransactions.col("user_id").equalTo(allUsers.col("user_id"))
                            .and(allTransactions.col("patch_id").equalTo(allUsers.col("patch_id"))));

            keyCounts.unpersist();

            return result.count();
        });

        spark.stop();
    }
}
