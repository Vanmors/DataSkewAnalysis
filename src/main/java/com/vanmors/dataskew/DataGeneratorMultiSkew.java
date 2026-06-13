package com.vanmors.dataskew;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Генератор данных с НЕСКОЛЬКИМИ скошенными ключами разной частоты.
 *
 * Доп. эксперимент: вместо одного hot key назначаем заданным user_id
 * заданные доли строк. Остаток распределяется равномерно по [0, numUsers).
 *
 * Пример (STRONG): hotKeyIds={42,7,13,99,256}, hotKeyFractions={0.30,0.20,0.15,0.10,0.05}
 * → 30% строк получат user_id=42, 20% — user_id=7, и т.д.; оставшиеся 20%
 * равномерно распределены по 1M пользователей.
 *
 * Схема transactions:
 *   transaction_id (long), user_id (long), amount (double)
 * Схема users:
 *   user_id (long), name (string)
 */
public class DataGeneratorMultiSkew {

    private final SparkSession spark;
    private final long numUsers;

    public DataGeneratorMultiSkew(final SparkSession spark, final long numUsers) {
        this.spark = spark;
        this.numUsers = numUsers;
    }

    public Dataset<Row> generateTransactions(final long totalRows,
                                             final long[] hotKeyIds,
                                             final double[] hotKeyFractions) {
        if (hotKeyIds.length != hotKeyFractions.length) {
            throw new IllegalArgumentException("hotKeyIds and hotKeyFractions must have equal length");
        }
        // Кумулятивные пороги: t[i] = sum(fractions[0..i])
        final double[] thresholds = new double[hotKeyIds.length];
        double cum = 0;
        for (int i = 0; i < hotKeyIds.length; i++) {
            cum += hotKeyFractions[i];
            thresholds[i] = cum;
        }
        if (cum > 1.0) {
            throw new IllegalArgumentException("Sum of hotKeyFractions must be <= 1.0, got " + cum);
        }

        // Два независимых случайных столбца: __sel — выбор hot/cold, __uid — равномерный user
        final Dataset<Row> ids = spark.range(0, totalRows).toDF("transaction_id")
                .withColumn("__sel", functions.rand())
                .withColumn("__uid", functions.rand());

        // Строим цепочку when(__sel < t0, hot0).when(__sel < t1, hot1)...otherwise(uniform)
        Column expr = functions.when(
                functions.col("__sel").lt(thresholds[0]),
                functions.lit(hotKeyIds[0]));
        for (int i = 1; i < hotKeyIds.length; i++) {
            expr = expr.when(
                    functions.col("__sel").lt(thresholds[i]),
                    functions.lit(hotKeyIds[i]));
        }
        expr = expr.otherwise(
                functions.floor(functions.col("__uid").multiply(numUsers)).cast(DataTypes.LongType));

        return ids
                .withColumn("user_id", expr)
                .withColumn("amount", functions.round(functions.rand().multiply(1000), 2))
                .drop("__sel", "__uid");
    }

    public Dataset<Row> generateUsers() {
        return spark.range(0, numUsers).toDF("user_id")
                .withColumn("name", functions.concat(functions.lit("user_"), functions.col("user_id")));
    }
}
