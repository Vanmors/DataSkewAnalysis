package com.vanmors.dataskew;

import com.vanmors.dataskew.AQE.AQEBenchmark;
import com.vanmors.dataskew.MapSideJoin.MapSideJoinBenchmark;
import com.vanmors.dataskew.PRPQ.PRPQBenchmark;
import com.vanmors.dataskew.PatchBasedRepartitioning.PatchBasedBenchmark;
import com.vanmors.dataskew.broadcast.BroadcastJoinBenchmark;
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
 * Единый оркестратор бенчмарков.
 *
 * SparkSession создаётся один раз. Для каждого из трёх уровней перекоса
 * данные генерируются один раз и используются всеми методами.
 * Baseline (обычный shuffle join) замеряется тоже один раз на уровень.
 * Все результаты собираются в единую таблицу и сохраняются в CSV.
 */
public class Main {

    private static final double[] SKEW_FRACTIONS = {0.20, 0.50, 0.80};
    private static final String[] SKEW_LABELS    = {"WEAK", "MEDIUM", "STRONG"};
    private static final String CSV_PATH = "/tmp/benchmark_results.csv";

    public static void main(final String[] args) {
        final SparkSession spark = BenchmarkUtils.createSparkSession("DataSkew Benchmark");
        final List<BenchmarkResult> allResults = new ArrayList<>();

        for (int i = 0; i < SKEW_FRACTIONS.length; i++) {
            final double fraction = SKEW_FRACTIONS[i];
            final String label = SKEW_LABELS[i];

            System.out.println();
            System.out.println("╔══════════════════════════════════════════════════════╗");
            System.out.printf( "║  Уровень перекоса: %-6s (%.0f%% строк на hot key)  ║%n", label, fraction * 100);
            System.out.println("╚══════════════════════════════════════════════════════╝");
            System.out.println();

            final Dataset<Row>[] data = BenchmarkUtils.generateAndCache(spark, fraction);
            final Dataset<Row> transactions = data[0];
            final Dataset<Row> users = data[1];

            // Baseline — обычный shuffle join без оптимизаций
            allResults.add(BenchmarkUtils.measure(spark, "Baseline (Shuffle Join)", label, fraction,
                    () -> transactions.join(users, "user_id").count()));

            allResults.add(AQEBenchmark.run(spark, transactions, users, label, fraction));
            allResults.add(SaltingBenchmark.run(spark, transactions, users, label, fraction));
            allResults.add(BroadcastJoinBenchmark.run(spark, transactions, users, label, fraction));
            allResults.add(PreAggregationBenchmark.run(spark, transactions, users, label, fraction));
            allResults.add(MapSideJoinBenchmark.run(spark, transactions, users, label, fraction));
            allResults.add(PRPQBenchmark.run(spark, transactions, users, label, fraction));
            allResults.add(PatchBasedBenchmark.run(spark, transactions, users, label, fraction));

            transactions.unpersist();
            users.unpersist();
        }

        printSummaryTable(allResults);
        saveCsv(allResults);

        spark.stop();
    }

    private static void printSummaryTable(final List<BenchmarkResult> results) {
        final String sep = "╠═══════════════════════════╪════════╪════════════╪══════════════╪══════════════╪════════════╪════════════╣";
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                                   СВОДНАЯ ТАБЛИЦА РЕЗУЛЬТАТОВ                                        ║");
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
            System.out.println("\nРезультаты сохранены в: " + CSV_PATH);
        } catch (final IOException e) {
            System.err.println("Не удалось сохранить CSV: " + e.getMessage());
        }
    }
}
