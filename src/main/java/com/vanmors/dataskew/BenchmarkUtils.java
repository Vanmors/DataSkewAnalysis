package com.vanmors.dataskew;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class BenchmarkUtils {

    public static final long TOTAL_TRANSACTIONS = 10_000_000;
    public static final int NUM_USERS = 1_000_000;
    public static final long HOT_KEY_ID = 42;
    public static final int WARMUP_RUNS = 1;
    public static final int MEASURED_RUNS = 3;

    // Путь к Hive Warehouse — общий volume, смонтированный на всех узлах кластера
    public static final String WAREHOUSE_DIR = "/opt/spark-warehouse/hive-warehouse";

    public static SparkSession createSparkSession(final String appName) {
        return SparkSession.builder()
                .appName(appName)
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
                // Derby Metastore хранится в том же shared volume; доступен только драйверу
                .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                        "jdbc:derby:/opt/spark-warehouse/metastore_db;create=true")
                .config("spark.hadoop.javax.jdo.option.ConnectionDriverName",
                        "org.apache.derby.jdbc.EmbeddedDriver")
                .config("spark.sql.sources.bucketing.enabled", "true")
                .config("spark.log.level", "WARN")
                .enableHiveSupport()
                .getOrCreate();
    }

    @SuppressWarnings("unchecked")
    public static Dataset<Row>[] generateAndCache(final SparkSession spark, final double skewFraction) {
        final DataGenerator gen = new DataGenerator(spark, NUM_USERS);
        final Dataset<Row> transactions = gen.generateTransactions(TOTAL_TRANSACTIONS, HOT_KEY_ID, skewFraction);
        final Dataset<Row> users = gen.generateUsers();

        transactions.cache().count();
        users.cache().count();

        System.out.printf("=== Данные сгенерированы (перекос: %.0f%%) ===%n", skewFraction * 100);
        System.out.println("Transactions: " + transactions.count() + " строк");
        System.out.println("Users:        " + users.count() + " строк");
        System.out.println("Hot key:      user_id=" + HOT_KEY_ID + " (~" + (int) (skewFraction * 100) + "% строк)");
        System.out.println();

        return new Dataset[]{transactions, users};
    }

    /**
     * Запускает WARMUP_RUNS прогревочных и MEASURED_RUNS замерных итераций.
     * Возвращает усреднённые метрики в виде BenchmarkResult.
     */
    public static BenchmarkResult measure(final SparkSession spark,
                                          final String method,
                                          final String skewLevel,
                                          final double skewFraction,
                                          final Supplier<Long> joinAction) {
        for (int i = 0; i < WARMUP_RUNS; i++) {
            joinAction.get();
        }

        final long[] times = new long[MEASURED_RUNS];
        long lastCount = 0;
        long totalShuffleReadSum = 0;
        long totalShuffleWriteSum = 0;
        long maxPeakMemory = 0;
        long maxTaskDurationSum = 0;

        for (int i = 0; i < MEASURED_RUNS; i++) {
            final MetricsListener listener = new MetricsListener();
            spark.sparkContext().addSparkListener(listener);

            final long startTime = System.currentTimeMillis();
            lastCount = joinAction.get();
            times[i] = System.currentTimeMillis() - startTime;

            spark.sparkContext().removeSparkListener(listener);

            totalShuffleReadSum += listener.totalShuffleRead.get();
            totalShuffleWriteSum += listener.totalShuffleWrite.get();
            maxPeakMemory = Math.max(maxPeakMemory, listener.maxPeakExecutionMemory.get());
            maxTaskDurationSum += listener.maxTaskDuration.get();
        }

        long avgTime = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        for (long t : times) {
            avgTime += t;
            minTime = Math.min(minTime, t);
            maxTime = Math.max(maxTime, t);
        }
        avgTime /= MEASURED_RUNS;

        final long avgShuffleRead = totalShuffleReadSum / MEASURED_RUNS;
        final long avgShuffleWrite = totalShuffleWriteSum / MEASURED_RUNS;
        final long avgMaxTaskDuration = maxTaskDurationSum / MEASURED_RUNS;

        System.out.printf("--- [%s | %.0f%%] %s ---%n", skewLevel, skewFraction * 100, method);
        System.out.println("  Строк результата:            " + lastCount);
        System.out.println("  Время (avg/min/max мс):      " + avgTime + " / " + minTime + " / " + maxTime);
        System.out.println("  Shuffle Read (avg):          " + formatBytes(avgShuffleRead));
        System.out.println("  Shuffle Write (avg):         " + formatBytes(avgShuffleWrite));
        System.out.println("  Peak Memory:                 " + formatBytes(maxPeakMemory));
        System.out.println("  Макс. задача (avg мс):       " + avgMaxTaskDuration);
        System.out.println();

        return new BenchmarkResult(method, skewLevel, skewFraction,
                avgTime, minTime, maxTime,
                avgShuffleRead, avgShuffleWrite,
                maxPeakMemory, avgMaxTaskDuration,
                lastCount);
    }

    public static class MetricsListener extends SparkListener {
        public final AtomicLong totalShuffleRead = new AtomicLong(0);
        public final AtomicLong totalShuffleWrite = new AtomicLong(0);
        public final AtomicLong maxPeakExecutionMemory = new AtomicLong(0);
        public final AtomicLong maxTaskDuration = new AtomicLong(0);
        public final AtomicLong totalTasks = new AtomicLong(0);
        public final AtomicLong totalStages = new AtomicLong(0);

        @Override
        public void onTaskEnd(final SparkListenerTaskEnd taskEnd) {
            if (taskEnd.taskMetrics() != null) {
                final var metrics = taskEnd.taskMetrics();
                totalShuffleRead.addAndGet(metrics.shuffleReadMetrics().totalBytesRead());
                totalShuffleWrite.addAndGet(metrics.shuffleWriteMetrics().bytesWritten());
                maxPeakExecutionMemory.accumulateAndGet(metrics.peakExecutionMemory(), Math::max);
                maxTaskDuration.accumulateAndGet(metrics.executorRunTime(), Math::max);
                totalTasks.incrementAndGet();
            }
        }

        @Override
        public void onStageCompleted(final SparkListenerStageCompleted stageCompleted) {
            totalStages.incrementAndGet();
        }
    }

    public static String formatBytes(final long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
