package com.vanmors.dataskew.salting;

import com.vanmors.dataskew.BenchmarkResult;
import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Бенчмарк метода Salting.
 *
 * К каждой строке transactions добавляется случайный суффикс-соль [0, NUM_SALT_BUCKETS).
 * Таблица users размножается (cross join) по всем значениям соли.
 * Join выполняется по составному ключу (user_id, salt), что равномерно
 * распределяет горячий ключ по NUM_SALT_BUCKETS партициям.
 */
public class SaltingBenchmark {

    private static final int NUM_SALT_BUCKETS = 10;

    public static BenchmarkResult run(final SparkSession spark,
                                      final Dataset<Row> transactions,
                                      final Dataset<Row> users,
                                      final String skewLevel,
                                      final double skewFraction) {
        return BenchmarkUtils.measure(spark, "Salting", skewLevel, skewFraction, () -> {
            final Dataset<Row> saltedTransactions = transactions
                    .withColumn("salt",
                            functions.floor(functions.rand().multiply(NUM_SALT_BUCKETS))
                                    .cast(DataTypes.IntegerType))
                    .withColumn("salted_user_id",
                            functions.concat(
                                    functions.col("user_id").cast(DataTypes.StringType),
                                    functions.lit("_"),
                                    functions.col("salt")));

            final Dataset<Row> saltRange = spark.range(0, NUM_SALT_BUCKETS).toDF("salt")
                    .withColumn("salt", functions.col("salt").cast(DataTypes.IntegerType));

            final Dataset<Row> explodedUsers = users.crossJoin(saltRange)
                    .withColumn("salted_user_id",
                            functions.concat(
                                    functions.col("user_id").cast(DataTypes.StringType),
                                    functions.lit("_"),
                                    functions.col("salt")));

            return saltedTransactions.join(explodedUsers,
                            saltedTransactions.col("salted_user_id").equalTo(explodedUsers.col("salted_user_id")))
                    .count();
        });
    }
}
