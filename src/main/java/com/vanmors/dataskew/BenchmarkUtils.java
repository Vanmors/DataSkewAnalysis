package com.vanmors.dataskew;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;


/**
 * Общие утилиты для всех бенчмарков перекоса данных.
 */
public class BenchmarkUtils {

    // --- параметры генерации данных ---
    public static final long TOTAL_TRANSACTIONS = 10_000_000;

    public static final int NUM_USERS = 1_000_000;

    public static final long HOT_KEY_ID = 42;

    public static final double HOT_KEY_FRACTION = 0.7;

    // --- параметры замеров ---
    public static final int WARMUP_RUNS = 1;

    public static final int MEASURED_RUNS = 3;

    /**
     * Создаёт SparkSession с отключённым AQE (для чистого замера метода).
     */
    public static SparkSession createSparkSession(final String appName) {

        return SparkSession.builder()
                .appName(appName)
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .config("spark.log.level", "WARN")
                .getOrCreate();
    }

    /**
     * Генерирует и кэширует тестовые данные, выводит статистику.
     */
    public static Dataset<Row>[] generateAndCache(final SparkSession spark) {
        final DataGenerator gen = new DataGenerator(spark, NUM_USERS);
        final Dataset<Row> transactions = gen.generateTransactions(TOTAL_TRANSACTIONS, HOT_KEY_ID, HOT_KEY_FRACTION);
        final Dataset<Row> users = gen.generateUsers();

        transactions.cache().count();
        users.cache().count();

        System.out.println("=== Данные сгенерированы ===");
        System.out.println("Transactions: " + transactions.count() + " строк");
        System.out.println("Users:        " + users.count() + " строк");
        System.out.println("Hot key:      user_id=" + HOT_KEY_ID + " (~" + (int)(HOT_KEY_FRACTION * 100) + "% строк)");
        System.out.println();

        @SuppressWarnings("unchecked") final Dataset<Row>[] result = new Dataset[] {transactions, users};
        return result;
    }

    /**
     * Замеряет выполнение join с прогревом и усреднением.
     *
     * Выполняет WARMUP_RUNS прогревочных запусков (без записи метрик),
     * затем MEASURED_RUNS замеров, усредняет результаты.
     */
    public static void measureAndPrint(final SparkSession spark, final String label,
                                       final Supplier<Long> joinAction) {
        // --- Warmup ---
        for (int i = 0; i < WARMUP_RUNS; i++) {
            joinAction.get();
        }

        // --- Measured runs ---
        long[] times = new long[MEASURED_RUNS];
        long lastCount = 0;
        MetricsListener lastListener = null;

        // Для усреднения shuffle-метрик
        long totalShuffleReadSum = 0;
        long totalShuffleWriteSum = 0;
        long maxPeakMemory = 0;
        long maxTaskDurationSum = 0;
        long totalTasksSum = 0;
        long totalStagesSum = 0;

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
            totalTasksSum += listener.totalTasks.get();
            totalStagesSum += listener.totalStages.get();
            lastListener = listener;
        }

        // Вычисляем средние
        long avgTime = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        for (long t : times) {
            avgTime += t;
            minTime = Math.min(minTime, t);
            maxTime = Math.max(maxTime, t);
        }
        avgTime /= MEASURED_RUNS;

        long avgShuffleRead = totalShuffleReadSum / MEASURED_RUNS;
        long avgShuffleWrite = totalShuffleWriteSum / MEASURED_RUNS;
        long avgMaxTaskDuration = maxTaskDurationSum / MEASURED_RUNS;
        long avgTasks = totalTasksSum / MEASURED_RUNS;
        long avgStages = totalStagesSum / MEASURED_RUNS;

        System.out.println("--- Результаты (" + label + ") [" + WARMUP_RUNS + " warmup + " + MEASURED_RUNS + " runs] ---");
        System.out.println("  Результат join:              " + lastCount + " строк");
        System.out.println("  Время (avg/min/max):         " + avgTime + " / " + minTime + " / " + maxTime + " мс");
        System.out.println("  Shuffle Read (avg):          " + formatBytes(avgShuffleRead));
        System.out.println("  Shuffle Write (avg):         " + formatBytes(avgShuffleWrite));
        System.out.println("  Peak Execution Memory:       " + formatBytes(maxPeakMemory));
        System.out.println("  Макс. задача (avg):          " + avgMaxTaskDuration + " мс");
        System.out.println("  Всего задач (avg):           " + avgTasks);
        System.out.println("  Всего стадий (avg):          " + avgStages);
        System.out.println();
    }

    /**
     * SparkListener для сбора метрик по задачам и стадиям.
     */
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

                final long peakMem = metrics.peakExecutionMemory();
                maxPeakExecutionMemory.accumulateAndGet(peakMem, Math::max);

                final long duration = metrics.executorRunTime();
                maxTaskDuration.accumulateAndGet(duration, Math::max);

                totalTasks.incrementAndGet();
            }
        }

        @Override
        public void onStageCompleted(final SparkListenerStageCompleted stageCompleted) {
            totalStages.incrementAndGet();
        }
    }

    public static String formatBytes(final long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
