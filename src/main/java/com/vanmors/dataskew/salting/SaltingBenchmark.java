package com.vanmors.dataskew.salting;

import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Бенчмарк метода Salting.
 *
 * Идея: добавить случайный суффикс (соль) к ключу join на стороне большой таблицы,
 * а малую таблицу размножить (explode) по всем значениям соли.
 * Это распределяет горячий ключ по нескольким партициям.
 *
 * Сравнение: обычный join vs. salted join.
 */
public class SaltingBenchmark {

    private static final int NUM_SALT_BUCKETS = 10;

    public static void main(String[] args) {
        SparkSession spark = BenchmarkUtils.createSparkSession("Salting Benchmark");

        Dataset<Row>[] data = BenchmarkUtils.generateAndCache(spark);
        Dataset<Row> transactions = data[0];
        Dataset<Row> users = data[1];

        // --- Базовый join (без salting) ---
        System.out.println(">>> Обычный JOIN (без salting) <<<");
        BenchmarkUtils.measureAndPrint(spark, "Без salting", () ->
                transactions.join(users, "user_id").count());

        // --- Salted join ---
        System.out.println(">>> JOIN с salting (buckets=" + NUM_SALT_BUCKETS + ") <<<");
        BenchmarkUtils.measureAndPrint(spark, "Salting", () -> {
            // Добавляем случайную соль к transactions
            Dataset<Row> saltedTransactions = transactions
                    .withColumn("salt",
                            functions.floor(functions.rand().multiply(NUM_SALT_BUCKETS))
                                    .cast(DataTypes.IntegerType))
                    .withColumn("salted_user_id",
                            functions.concat(functions.col("user_id").cast(DataTypes.StringType),
                                    functions.lit("_"),
                                    functions.col("salt")));

            // Размножаем users по всем значениям соли
            Dataset<Row> saltRange = spark.range(0, NUM_SALT_BUCKETS)
                    .toDF("salt")
                    .withColumn("salt", functions.col("salt").cast(DataTypes.IntegerType));

            Dataset<Row> explodedUsers = users
                    .crossJoin(saltRange)
                    .withColumn("salted_user_id",
                            functions.concat(functions.col("user_id").cast(DataTypes.StringType),
                                    functions.lit("_"),
                                    functions.col("salt")));

            // Join по salted_user_id
            return saltedTransactions.join(explodedUsers,
                    saltedTransactions.col("salted_user_id").equalTo(explodedUsers.col("salted_user_id")))
                    .count();
        });

        spark.stop();
    }
}
