import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class UpdateIMT {
    private static final int THREADS = 4;
    private static final String FILE_PATH = "/Users/manfred/Desktop/pgbench_accounts.csv";
    private static final String TEMP_PATH = "/Users/manfred/Desktop/pgbench_accounts_tmp.csv";

    public static void main(String[] args) throws Exception {
        List<String> allLines = new ArrayList<>();

        // 读取文件
        try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                allLines.add(line);
            }
        }

        long start = System.currentTimeMillis();

        // 并行更新
        int chunkSize = (int) Math.ceil(allLines.size() * 1.0 / THREADS);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<Integer>> results = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            int startIdx = t * chunkSize;
            int endIdx = Math.min(startIdx + chunkSize, allLines.size());
            List<String> subList = allLines.subList(startIdx, endIdx);
            results.add(pool.submit(() -> updateRange(subList)));
        }

        int totalUpdated = 0;
        for (Future<Integer> f : results) {
            totalUpdated += f.get();
        }
        pool.shutdown();

        // 写入临时文件
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(TEMP_PATH))) {
            for (String line : allLines) {
                bw.write(line);
                bw.newLine();
            }
        }

        // 替换
        File oldFile = new File(FILE_PATH);
        File tmpFile = new File(TEMP_PATH);

        if (!tmpFile.renameTo(oldFile)) {
            oldFile.delete();
            if (!tmpFile.renameTo(oldFile)) {
                throw new IOException("Failed to overwrite original file!");
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("Total updated rows: " + totalUpdated);
        System.out.println("Update + write-back time: " + (end - start) + " ms");
    }

    private static int updateRange(List<String> rows) {
        int updated = 0;
        for (int i = 0; i < rows.size(); i++) {
            String line = rows.get(i);
            String[] parts = line.split(",");
            try {
                int aid = Integer.parseInt(parts[0].trim());
                int abalance = Integer.parseInt(parts[2].trim());
                if (aid >= 1 && aid <= 100000) {
                    abalance += 1;
                    parts[2] = String.valueOf(abalance);
                    rows.set(i, String.join(",", parts));
                    updated++;
                }
            } catch (Exception ignored) {}
        }
        return updated;
    }
}
