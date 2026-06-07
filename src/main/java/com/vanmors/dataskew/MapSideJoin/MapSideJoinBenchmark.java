package com.vanmors.dataskew.MapSideJoin;

import com.vanmors.dataskew.BenchmarkResult;
import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Бенчмарк Map-side Join (Bucketed Sort-Merge Join).
 *
 * Полноценная реализация через Hive Metastore (Apache Derby, embedded):
 *   1) Обе таблицы записываются с bucketBy(NUM_BUCKETS, "user_id") + sortBy("user_id")
 *      в Hive Warehouse через saveAsTable(). Метаданные о разбиении сохраняются
 *      в Derby Metastore.
 *   2) При чтении через spark.table() Spark знает о бакетировании.
 *   3) Если число бакетов совпадает у обеих таблиц, Spark исключает Exchange (shuffle)
 *      из физического плана — join выполняется без сетевой пересылки данных.
 *
 * Warehouse dir и Derby Metastore размещены на shared Docker volume (/opt/spark-warehouse),
 * примонтированном ко всем узлам кластера по одному пути. Воркеры читают файлы бакетов
 * напрямую из volume; к Derby Metastore обращается только драйвер.
 *
 * Стоимость записи bucketed-таблиц в замер не включается — в production bucketing
 * выполняется один раз при ETL и амортизируется по всем последующим запросам.
 */
public class MapSideJoinBenchmark {

    private static final int NUM_BUCKETS = 200;
    private static final String TX_TABLE    = "benchmark_transactions_bucketed";
    private static final String USERS_TABLE = "benchmark_users_bucketed";

    public static BenchmarkResult run(final SparkSession spark,
                                      final Dataset<Row> transactions,
                                      final Dataset<Row> users,
                                      final String skewLevel,
                                      final double skewFraction) {
        setupBucketedTables(spark, transactions, users);

        final Dataset<Row> buckTx    = spark.table(TX_TABLE);
        final Dataset<Row> buckUsers = spark.table(USERS_TABLE);

        printJoinPlan(spark, buckTx, buckUsers);

        final BenchmarkResult result = BenchmarkUtils.measure(
                spark, "Map-side Join (Bucketed)", skewLevel, skewFraction,
                () -> buckTx.join(buckUsers, "user_id").count());

        cleanup(spark);
        return result;
    }

    private static void setupBucketedTables(final SparkSession spark,
                                            final Dataset<Row> transactions,
                                            final Dataset<Row> users) {
        System.out.println("  [Map-side] Запись bucketed-таблиц в Hive Warehouse...");

        spark.sql("DROP TABLE IF EXISTS " + TX_TABLE);
        spark.sql("DROP TABLE IF EXISTS " + USERS_TABLE);

        transactions.write()
                .mode(SaveMode.Overwrite)
                .bucketBy(NUM_BUCKETS, "user_id")
                .sortBy("user_id")
                .saveAsTable(TX_TABLE);

        users.write()
                .mode(SaveMode.Overwrite)
                .bucketBy(NUM_BUCKETS, "user_id")
                .sortBy("user_id")
                .saveAsTable(USERS_TABLE);

        System.out.println("  [Map-side] Таблицы записаны (" + NUM_BUCKETS + " бакетов каждая).");
    }

    // Печатает физический план join-а один раз — позволяет убедиться,
    // что Exchange (shuffle) отсутствует в плане.
    private static void printJoinPlan(final SparkSession spark,
                                      final Dataset<Row> buckTx,
                                      final Dataset<Row> buckUsers) {
        System.out.println("  [Map-side] Физический план join-а (проверка отсутствия Exchange):");
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");
        buckTx.join(buckUsers, "user_id").explain(false);
    }

    private static void cleanup(final SparkSession spark) {
        spark.sql("DROP TABLE IF EXISTS " + TX_TABLE);
        spark.sql("DROP TABLE IF EXISTS " + USERS_TABLE);
    }
}
