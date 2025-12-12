import java.io.*;
import java.util.concurrent.*;
import java.util.*;

public class RetrievalParallel {
    private static final int THREADS = Runtime.getRuntime().availableProcessors();
    private static final long THRESHOLD = 5000;
    private static final String FILE = "pgbench_accounts.csv";

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        long fileSize = new File(FILE).length();
        long chunkSize = fileSize / THREADS;

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<Long>> results = new ArrayList<>();

        for (int i = 0; i < THREADS; i++) {
            final long startPos = i * chunkSize;
            final long endPos = (i == THREADS - 1) ? fileSize : (i + 1) * chunkSize;
            results.add(pool.submit(() -> processChunk(startPos, endPos)));
        }

        long totalCount = 0;
        for (Future<Long> f : results) totalCount += f.get();
        pool.shutdown();

        long end = System.currentTimeMillis();
        System.out.println("Count of abalance > " + THRESHOLD + ": " + totalCount);
        System.out.println("Threads: " + THREADS);
        System.out.println("Search time: " + (end - start) + " ms");
    }

    private static long processChunk(long start, long end) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(FILE, "r");
        raf.seek(start);

        // 对齐到行首
        if (start != 0) raf.readLine();

        long count = 0;
        String line;
        while ((line = raf.readLine()) != null) {
            long pos = raf.getFilePointer();
            if (pos > end) break;

            int firstComma = line.indexOf(',');
            int secondComma = line.indexOf(',', firstComma + 1);
            int thirdComma = line.indexOf(',', secondComma + 1);
            if (secondComma == -1) continue;
            try {
                long abalance = Long.parseLong(line.substring(secondComma + 1, thirdComma == -1 ? line.length() : thirdComma).trim());
                if (abalance > THRESHOLD) count++;
            } catch (NumberFormatException e) {}
        }
        raf.close();
        return count;
    }
}
