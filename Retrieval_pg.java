import java.io.*;

public class Retrieval_pg {
    public static void main(String[] args) throws Exception {
        String filePath = "pgbench_accounts.csv";
        long threshold = 5000;
        long count = 0;

        long start = System.currentTimeMillis();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath), 1 << 20)) { // 1MB buffer
            String line;
            while ((line = br.readLine()) != null) {
                int firstComma = line.indexOf(',');
                int secondComma = line.indexOf(',', firstComma + 1);
                int thirdComma = line.indexOf(',', secondComma + 1);
                if (secondComma == -1) continue;
                try {
                    long abalance = Long.parseLong(line.substring(secondComma + 1, thirdComma == -1 ? line.length() : thirdComma).trim());
                    if (abalance > threshold) count++;
                } catch (NumberFormatException e) {}
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("Count of abalance > " + threshold + ": " + count);
        System.out.println("Search time: " + (end - start) + " ms");
    }
}
