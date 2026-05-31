package com.vanmors.dataskew;

import com.vanmors.dataskew.AQE.AQEBenchmark;
import com.vanmors.dataskew.broadcast.BroadcastJoinBenchmark;
import com.vanmors.dataskew.preaggregation.PreAggregationBenchmark;
import com.vanmors.dataskew.salting.SaltingBenchmark;
import com.vanmors.dataskew.PRPQ.PRPQBenchmark;
import com.vanmors.dataskew.PatchBasedRepartitioning.PatchBasedBenchmark;
import com.vanmors.dataskew.MapSideJoin.MapSideJoinBenchmark;

/**
 * Запуск всех бенчмарков последовательно.
 */
public class Main {
    public static void main(String[] args) {
        String[] empty = new String[0];

        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║    ЗАПУСК ВСЕХ БЕНЧМАРКОВ DATA SKEW         ║");
        System.out.println("╚══════════════════════════════════════════════╝");
        System.out.println();

        runBenchmark("1. AQE (Adaptive Query Execution)", () -> AQEBenchmark.main(empty));
        runBenchmark("2. Salting", () -> SaltingBenchmark.main(empty));
        runBenchmark("3. Broadcast Join", () -> BroadcastJoinBenchmark.main(empty));
        runBenchmark("4. Pre-aggregation", () -> PreAggregationBenchmark.main(empty));
        runBenchmark("5. Map-side Join (Bucketed)", () -> MapSideJoinBenchmark.main(empty));
        runBenchmark("6. PRPQ", () -> PRPQBenchmark.main(empty));
        runBenchmark("7. Patch-based Repartitioning", () -> PatchBasedBenchmark.main(empty));

        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║    ВСЕ БЕНЧМАРКИ ЗАВЕРШЕНЫ                  ║");
        System.out.println("╚══════════════════════════════════════════════╝");
    }

    private static void runBenchmark(String name, Runnable benchmark) {
        System.out.println("========================================");
        System.out.println("  " + name);
        System.out.println("========================================");
        try {
            benchmark.run();
        } catch (Exception e) {
            System.out.println("  ОШИБКА: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println();
    }
}
