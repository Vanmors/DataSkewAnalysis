package com.vanmors.dataskew.MapSideJoin;

import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Бенчмарк Map-side Join (имитация Bucketed Join).
 *
 * Идея: обе таблицы заранее со-разбиваются (co-partitioned) по ключу join
 * на одинаковое число партиций. Это позволяет выполнить join без
 * дополнительного shuffle, т.к. данные уже распределены по одному ключу.
 *
 * В продуктовой среде bucketed join реализуется через Hive Metastore
 * (saveAsTable + bucketBy). В данном бенчмарке используется
 * repartition() + кэширование как приближение этого подхода.
 *
 * Примечание: Spark-оптимизатор может всё ещё выполнять shuffle,
 * т.к. без Hive-метаданных он не знает о со-разбиении.
 * Полноценный bucketed join требует enableHiveSupport() и hive-shims.
 *
 * Сравнение: обычный join vs. со-партиционированный join.
 */
public class MapSideJoinBenchmark {

    private static final int NUM_BUCKETS = 200;

    public static void main(String[] args) {
        SparkSession spark = BenchmarkUtils.createSparkSession("Map-side Join Benchmark");

        Dataset<Row>[] data = BenchmarkUtils.generateAndCache(spark);
        Dataset<Row> transactions = data[0];
        Dataset<Row> users = data[1];

        // --- Обычный join ---
        System.out.println(">>> Обычный JOIN <<<");
        BenchmarkUtils.measureAndPrint(spark, "Shuffle Join", () ->
                transactions.join(users, "user_id").count());

        // --- Co-partitioned join ---
        System.out.println(">>> Co-partitioned (Map-side) JOIN <<<");

        // Со-разбиваем обе таблицы по user_id на одинаковое число партиций
        // и кэшируем, чтобы при join не было повторного shuffle
        Dataset<Row> repartTransactions = transactions
                .repartition(NUM_BUCKETS, functions.col("user_id"))
                .sortWithinPartitions("user_id");
        Dataset<Row> repartUsers = users
                .repartition(NUM_BUCKETS, functions.col("user_id"))
                .sortWithinPartitions("user_id");

        repartTransactions.cache().count();
        repartUsers.cache().count();

        BenchmarkUtils.measureAndPrint(spark, "Co-partitioned Join", () ->
                repartTransactions.join(repartUsers, "user_id").count());

        repartTransactions.unpersist();
        repartUsers.unpersist();

        spark.stop();
    }
}
