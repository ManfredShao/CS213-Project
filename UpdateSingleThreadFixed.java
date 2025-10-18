import java.io.*;
import java.util.*;

public class UpdateSingleThreadFixed {
    private static final String FILE_PATH = "/Users/manfred/Desktop/pgbench_accounts.csv";
    private static final String TEMP_PATH = "/Users/manfred/Desktop/pgbench_accounts_tmp.csv";

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        int updated = 0;
        List<String> updatedLines = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                try {
                    int aid = Integer.parseInt(parts[0].trim());
                    int abalance = Integer.parseInt(parts[2].trim());
                    if (aid >= 1 && aid <= 100000) {
                        abalance += 1;
                        parts[2] = String.valueOf(abalance);
                        updated++;
                    }
                    line = String.join(",", parts);
                } catch (Exception ignored) {}
                updatedLines.add(line);
            }
        }

        // 写回修改后的文件
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(TEMP_PATH))) {
            for (String l : updatedLines) {
                bw.write(l);
                bw.newLine();
            }
        }

        // 安全覆盖旧文件
        File oldFile = new File(FILE_PATH);
        File tmpFile = new File(TEMP_PATH);
        if (!tmpFile.renameTo(oldFile)) {
            oldFile.delete();
            if (!tmpFile.renameTo(oldFile)) {
                throw new IOException("Failed to replace original file!");
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("Total updated rows: " + updated);
        System.out.println("Execution time: " + (end - start) + " ms");
    }
}
