package com.vanmors.dataskew.broadcast;

import com.vanmors.dataskew.BenchmarkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Бенчмарк Broadcast Join.
 *
 * Идея: малая таблица (users) целиком рассылается на все исполнители
 * через broadcast. Join выполняется локально без shuffle.
 *
 * Подходит, когда одна из таблиц достаточно мала, чтобы поместиться
 * в памяти каждого исполнителя.
 *
 * Сравнение: обычный shuffle join vs. broadcast join.
 */
public class BroadcastJoinBenchmark {

    public static void main(String[] args) {
        SparkSession spark = BenchmarkUtils.createSparkSession("Broadcast Join Benchmark");

        Dataset<Row>[] data = BenchmarkUtils.generateAndCache(spark);
        Dataset<Row> transactions = data[0];
        Dataset<Row> users = data[1];

        // --- Обычный shuffle join ---
        System.out.println(">>> Обычный shuffle JOIN <<<");
        BenchmarkUtils.measureAndPrint(spark, "Shuffle Join", () ->
                transactions.join(users, "user_id").count());

        // --- Broadcast join ---
        System.out.println(">>> Broadcast JOIN <<<");
        BenchmarkUtils.measureAndPrint(spark, "Broadcast Join", () ->
                transactions.join(functions.broadcast(users), "user_id").count());

        spark.stop();
    }
}
