package com.vanmors.dataskew;

import com.vanmors.dataskew.AQE.AQEBenchmark;
import com.vanmors.dataskew.MapSideJoin.MapSideJoinBenchmark;
import com.vanmors.dataskew.PRPQ.PRPQBenchmark;
import com.vanmors.dataskew.PatchBasedRepartitioning.PatchBasedBenchmark;
import com.vanmors.dataskew.broadcast.BroadcastJoinBenchmark;
import com.vanmors.dataskew.PatchBasedRepartitioning.PatchBasedBenchmarkV2;
import com.vanmors.dataskew.PatchBasedRepartitioning.PatchBasedBenchmarkV3;
import com.vanmors.dataskew.preaggregation.PreAggregationBenchmark;
import com.vanmors.dataskew.salting.SaltingBenchmark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Дополнительный эксперимент: бенчмарки на данных с несколькими скошенными ключами.
 *
 * В отличие от Main.java (один hot key id=42), здесь 5 hot keys с убывающей частотой.
 * Цель — показать преимущества методов, адаптивных к индивидуальной частоте ключей
 * (Patch-based, PRPQ), и пределы фиксированного salt count у Salting.
 *
 * Дополнительно сравниваются три варианта Patch-based (PATCH_SIZE = 50K / 500K / 1M)
 * для оценки чувствительности к этому параметру.
 *
 * Результаты сохраняются в отдельный CSV: benchmark_results_multiskew.csv.
 */
public class MainMultiSkew {

    private static final String[] SKEW_LABELS = {"WEAK", "MEDIUM", "STRONG"};
    private static final double[] TOTAL_SKEW_FRACTIONS = {0.20, 0.50, 0.80};

    private static final long[] HOT_KEY_IDS = {42L, 7L, 13L, 99L, 256L};

    // [уровень перекоса][hot key] -> доля строк
    private static final double[][] HOT_KEY_FRACTIONS = {
            {0.07, 0.05, 0.04, 0.02, 0.02}, // WEAK  Σ=0.20
            {0.18, 0.13, 0.10, 0.06, 0.03}, // MED   Σ=0.50
            {0.30, 0.20, 0.15, 0.10, 0.05}, // STR   Σ=0.80
    };

    private static final String CSV_PATH = "/tmp/benchmark_results_multiskew.csv";

    public static void main(final String[] args) {
        final SparkSession spark = BenchmarkUtils.createSparkSession("DataSkew Benchmark — Multi-Key");
        final List<BenchmarkResult> allResults = new ArrayList<>();

        for (int i = 0; i < SKEW_LABELS.length; i++) {
            final String label = SKEW_LABELS[i];
            final double totalFraction = TOTAL_SKEW_FRACTIONS[i];
            final double[] fractions = HOT_KEY_FRACTIONS[i];

            System.out.println();
            System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
            System.out.printf( "║  Multi-skew %-6s (Σ=%.2f, %d hot keys)                              ║%n",
                    label, totalFraction, HOT_KEY_IDS.length);
            System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
            printDistribution(fractions);

            final Dataset<Row>[] data = generateAndCache(spark, label, fractions);
            final Dataset<Row> transactions = data[0];
            final Dataset<Row> users = data[1];

            allResults.add(BenchmarkUtils.measure(spark, "Baseline (Shuffle Join)", label, totalFraction,
                    () -> transactions.join(users, "user_id").count()));

            allResults.add(AQEBenchmark.run(spark, transactions, users, label, totalFraction));
            allResults.add(SaltingBenchmark.run(spark, transactions, users, label, totalFraction));
            allResults.add(BroadcastJoinBenchmark.run(spark, transactions, users, label, totalFraction));
            allResults.add(PreAggregationBenchmark.run(spark, transactions, users, label, totalFraction));
            allResults.add(MapSideJoinBenchmark.run(spark, transactions, users, label, totalFraction));
            allResults.add(PRPQBenchmark.run(spark, transactions, users, label, totalFraction));
            allResults.add(PatchBasedBenchmark.run(spark, transactions, users, label, totalFraction));
            allResults.add(PatchBasedBenchmarkV2.run(spark, transactions, users, label, totalFraction));
            allResults.add(PatchBasedBenchmarkV3.run(spark, transactions, users, label, totalFraction));

            transactions.unpersist();
            users.unpersist();
        }

        printSummaryTable(allResults);
        saveCsv(allResults);

        spark.stop();
    }

    @SuppressWarnings("unchecked")
    private static Dataset<Row>[] generateAndCache(final SparkSession spark,
                                                   final String label,
                                                   final double[] fractions) {
        final DataGeneratorMultiSkew gen = new DataGeneratorMultiSkew(spark, BenchmarkUtils.NUM_USERS);
        final Dataset<Row> transactions = gen.generateTransactions(
                BenchmarkUtils.TOTAL_TRANSACTIONS, HOT_KEY_IDS, fractions);
        final Dataset<Row> users = gen.generateUsers();

        transactions.cache().count();
        users.cache().count();

        System.out.printf("=== Multi-skew данные сгенерированы (%s) ===%n", label);
        System.out.println("Transactions: " + transactions.count() + " строк");
        System.out.println("Users:        " + users.count() + " строк");
        System.out.println();

        return new Dataset[]{transactions, users};
    }

    private static void printDistribution(final double[] fractions) {
        final StringBuilder sb = new StringBuilder("  Распределение:  ");
        for (int j = 0; j < HOT_KEY_IDS.length; j++) {
            sb.append(String.format("user_id=%d → %.0f%%", HOT_KEY_IDS[j], fractions[j] * 100));
            if (j < HOT_KEY_IDS.length - 1) sb.append(", ");
        }
        System.out.println(sb);
        System.out.println();
    }

    private static void printSummaryTable(final List<BenchmarkResult> results) {
        final String sep = "╠═══════════════════════════╪════════╪════════════╪══════════════╪══════════════╪════════════╪════════════╣";
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                           СВОДНАЯ ТАБЛИЦА (MULTI-SKEW EXPERIMENT)                                    ║");
        System.out.println("╠═══════════════════════════╤════════╤════════════╤══════════════╤══════════════╤════════════╤════════════╣");
        System.out.printf( "║ %-25s │ %-6s │ %10s │ %12s │ %12s │ %10s │ %10s ║%n",
                "Метод", "Перекос", "Время мс", "Shuffle Read", "Shuffle Write", "Peak Mem", "Макс зад мс");
        System.out.println(sep);

        String lastSkew = "";
        for (final BenchmarkResult r : results) {
            if (!r.skewLevel.equals(lastSkew) && !lastSkew.isEmpty()) {
                System.out.println(sep);
            }
            lastSkew = r.skewLevel;
            System.out.printf("║ %-25s │ %-6s │ %10d │ %12s │ %12s │ %10s │ %10d ║%n",
                    r.method, r.skewLevel, r.avgTimeMs,
                    BenchmarkUtils.formatBytes(r.avgShuffleReadBytes),
                    BenchmarkUtils.formatBytes(r.avgShuffleWriteBytes),
                    BenchmarkUtils.formatBytes(r.peakMemoryBytes),
                    r.avgMaxTaskMs);
        }
        System.out.println("╚═══════════════════════════╧════════╧════════════╧══════════════╧══════════════╧════════════╧════════════╝");
    }

    private static void saveCsv(final List<BenchmarkResult> results) {
        try (final PrintWriter pw = new PrintWriter(new FileWriter(CSV_PATH))) {
            pw.println(BenchmarkResult.csvHeader());
            for (final BenchmarkResult r : results) {
                pw.println(r.toCsvRow());
            }
            System.out.println("\nMulti-skew результаты сохранены в: " + CSV_PATH);
        } catch (final IOException e) {
            System.err.println("Не удалось сохранить CSV: " + e.getMessage());
        }
    }
}
