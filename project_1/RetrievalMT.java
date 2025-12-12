import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.*;

public class RetrievalMT {
    private static final int THREADS = Runtime.getRuntime().availableProcessors();
    private static final long THRESHOLD = 5000;
    private static final String FILE = "/Users/manfred/Desktop/pgbench_accounts.csv";

    public static void main(String[] args) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        long start = System.currentTimeMillis();

        BufferedReader br = new BufferedReader(new FileReader(FILE), 1 << 20);
        String line;
        long total = 0;
        int batchSize = 10000;
        long count = 0;
        var futures = new java.util.ArrayList<Future<Long>>();
        var batch = new java.util.ArrayList<String>(batchSize);

        while ((line = br.readLine()) != null) {
            batch.add(line);
            if (batch.size() == batchSize) {
                var task = new ArrayList<>(batch);
                futures.add(pool.submit(() -> processBatch(task)));
                batch.clear();
            }
        }
        if (!batch.isEmpty()) futures.add(pool.submit(() -> processBatch(batch)));

        for (var f : futures) total += f.get();
        pool.shutdown();
        br.close();

        long end = System.currentTimeMillis();
        System.out.println("Count of abalance > " + THRESHOLD + ": " + total);
        System.out.println("Threads: " + THREADS);
        System.out.println("Time: " + (end - start) + " ms");
    }

    private static long processBatch(java.util.List<String> lines) {
        long count = 0;
        for (String line : lines) {
            int firstComma = line.indexOf(',');
            int secondComma = line.indexOf(',', firstComma + 1);
            int thirdComma = line.indexOf(',', secondComma + 1);
            if (secondComma == -1) continue;
            try {
                long abalance = Long.parseLong(line.substring(secondComma + 1,
                        thirdComma == -1 ? line.length() : thirdComma).trim());
                if (abalance > THRESHOLD) count++;
            } catch (NumberFormatException ignored) {}
        }
        return count;
    }
}
