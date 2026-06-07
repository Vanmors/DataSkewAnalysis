package com.vanmors.dataskew;

public class BenchmarkResult {
    public final String method;
    public final String skewLevel;
    public final double skewFraction;
    public final long avgTimeMs;
    public final long minTimeMs;
    public final long maxTimeMs;
    public final long avgShuffleReadBytes;
    public final long avgShuffleWriteBytes;
    public final long peakMemoryBytes;
    public final long avgMaxTaskMs;
    public final long resultCount;

    public BenchmarkResult(String method, String skewLevel, double skewFraction,
                           long avgTimeMs, long minTimeMs, long maxTimeMs,
                           long avgShuffleReadBytes, long avgShuffleWriteBytes,
                           long peakMemoryBytes, long avgMaxTaskMs, long resultCount) {
        this.method = method;
        this.skewLevel = skewLevel;
        this.skewFraction = skewFraction;
        this.avgTimeMs = avgTimeMs;
        this.minTimeMs = minTimeMs;
        this.maxTimeMs = maxTimeMs;
        this.avgShuffleReadBytes = avgShuffleReadBytes;
        this.avgShuffleWriteBytes = avgShuffleWriteBytes;
        this.peakMemoryBytes = peakMemoryBytes;
        this.avgMaxTaskMs = avgMaxTaskMs;
        this.resultCount = resultCount;
    }

    public static String csvHeader() {
        return "method,skew_level,skew_fraction,avg_time_ms,min_time_ms,max_time_ms," +
               "avg_shuffle_read_bytes,avg_shuffle_write_bytes,peak_memory_bytes,avg_max_task_ms,result_count";
    }

    public String toCsvRow() {
        return String.join(",",
                escapeCsv(method),
                escapeCsv(skewLevel),
                String.format("%.2f", skewFraction),
                String.valueOf(avgTimeMs),
                String.valueOf(minTimeMs),
                String.valueOf(maxTimeMs),
                String.valueOf(avgShuffleReadBytes),
                String.valueOf(avgShuffleWriteBytes),
                String.valueOf(peakMemoryBytes),
                String.valueOf(avgMaxTaskMs),
                String.valueOf(resultCount));
    }

    private static String escapeCsv(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
