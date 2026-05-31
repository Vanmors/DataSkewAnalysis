package com.vanmors.dataskew;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;


/**
 * Генератор двух синтетических наборов данных со скошенным распределением user_id.
 * <p>
 * Таблица transactions (большая, скошенная):
 * - transaction_id (long)
 * - user_id        (long)  — контролируемый перекос: заданная доля строк приходится на «горячие» ключи
 * - amount         (double)
 * <p>
 * Таблица users (справочник, равномерное распределение):
 * - user_id (long)
 * - name    (string)
 */
public class DataGenerator {

    private final SparkSession spark;

    private final long numUsers;

    /**
     * @param numUsers количество уникальных user_id
     */
    public DataGenerator(final SparkSession spark, final long numUsers) {
        this.spark = spark;
        this.numUsers = numUsers;
    }

    /**
     * Генерирует таблицу transactions с перекосом.
     *
     * @param totalRows      общее число строк
     * @param hotKeyId       id «горячего» ключа
     * @param hotKeyFraction доля строк, приходящаяся на горячий ключ (0..1)
     */
    public Dataset<Row> generateTransactions(final long totalRows, final long hotKeyId, final double hotKeyFraction) {
        final Dataset<Row> ids = spark.range(0, totalRows).toDF("transaction_id");

        // Для каждой строки: с вероятностью hotKeyFraction назначаем hotKeyId,
        // иначе — случайный user_id из [0, numUsers)
        final Dataset<Row> transactions = ids
                .withColumn("user_id",
                        functions.when(functions.rand().lt(hotKeyFraction), functions.lit(hotKeyId))
                                .otherwise(functions.floor(functions.rand().multiply(numUsers)).cast(DataTypes.LongType)))
                .withColumn("amount",
                        functions.round(functions.rand().multiply(1000), 2));

        return transactions;
    }

    /**
     * Генерирует справочную таблицу users (равномерное распределение).
     */
    public Dataset<Row> generateUsers() {
        final Dataset<Row> users = spark.range(0, numUsers).toDF("user_id")
                .withColumn("name", functions.concat(functions.lit("user_"), functions.col("user_id")));

        return users;
    }
}
