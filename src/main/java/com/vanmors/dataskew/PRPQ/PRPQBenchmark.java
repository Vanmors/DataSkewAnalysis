package com.vanmors.dataskew.PRPQ;

import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Бенчмарк алгоритма PRPQ (Partial Redistribution & Partial Query).
 * На основе: Cheng et al., 2014.
 *
 * Идея алгоритма:
 *   1) Определить «тяжёлые» ключи (heavy hitters) в большой таблице —
 *      ключи, чья частота превышает порог.
 *   2) Для тяжёлых ключей: выполнить broadcast join —
 *      соответствующие строки из малой таблицы рассылаются на все
 *      исполнители (Partial Redistribution).
 *   3) Для остальных ключей: выполнить обычный shuffle join
 *      (Partial Query).
 *   4) Объединить результаты через union.
 *
 * Преимущество: shuffle выполняется только для нескошенной части данных,
 * а тяжёлые ключи обрабатываются без shuffle.
 *
 * Сравнение: обычный join vs. PRPQ join.
 */
public class PRPQBenchmark {

    // Порог для определения тяжёлого ключа: если частота ключа
    // составляет более этой доли от общего числа строк
    private static final double HEAVY_KEY_THRESHOLD = 0.01;

    public static void main(final String[] args) {
        final SparkSession spark = BenchmarkUtils.createSparkSession("PRPQ Benchmark");

        final Dataset<Row>[] data = BenchmarkUtils.generateAndCache(spark);
        final Dataset<Row> transactions = data[0];
        final Dataset<Row> users = data[1];

        final long totalTransactions = transactions.count();

        // --- Обычный join ---
        System.out.println(">>> Обычный JOIN <<<");
        BenchmarkUtils.measureAndPrint(spark, "Shuffle Join", () ->
                transactions.join(users, "user_id").count());

        // --- PRPQ join ---
        System.out.println(">>> PRPQ JOIN <<<");
        BenchmarkUtils.measureAndPrint(spark, "PRPQ", () -> {

            // Шаг 1: определяем тяжёлые ключи
            final long threshold = (long) (totalTransactions * HEAVY_KEY_THRESHOLD);

            final Dataset<Row> keyCounts = transactions
                    .groupBy("user_id")
                    .agg(functions.count("*").alias("cnt"))
                    .filter(functions.col("cnt").gt(threshold));

            // Собираем тяжёлые ключи (их немного — единицы/десятки)
            final Dataset<Row> heavyKeys = keyCounts.select("user_id");
            heavyKeys.cache().count();

            // Шаг 2: разделяем transactions на тяжёлую и лёгкую части
            final Dataset<Row> txHeavy = transactions.join(heavyKeys, "user_id");
            final Dataset<Row> txLight = transactions.join(heavyKeys,
                    transactions.col("user_id").equalTo(heavyKeys.col("user_id")), "left_anti");

            // Шаг 3: для тяжёлых ключей — broadcast join с users
            final Dataset<Row> usersForHeavy = users.join(heavyKeys, "user_id");
            final Dataset<Row> resultHeavy = txHeavy.join(
                    functions.broadcast(usersForHeavy), "user_id");

            // Шаг 4: для лёгких ключей — обычный shuffle join
            final Dataset<Row> resultLight = txLight.join(users, "user_id");

            // Шаг 5: объединяем результаты
            final Dataset<Row> resultHeavyAligned = resultHeavy.select(
                    "user_id", "transaction_id", "amount", "name");
            final Dataset<Row> resultLightAligned = resultLight.select(
                    "user_id", "transaction_id", "amount", "name");

            heavyKeys.unpersist();

            return resultHeavyAligned.unionAll(resultLightAligned).count();
        });

        spark.stop();
    }
}
