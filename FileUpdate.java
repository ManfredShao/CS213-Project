import java.io.*;
import java.nio.file.*;

public class FileUpdate {
    public static void main(String[] args) throws Exception {
        Path path = Paths.get("movies_large.csv");
        Path tempPath = Paths.get("movies_temp.csv");

        long start = System.currentTimeMillis();
        try (BufferedReader br = Files.newBufferedReader(path);
             BufferedWriter bw = Files.newBufferedWriter(tempPath)) {
            String line;
            while ((line = br.readLine()) != null) {
                bw.write(line.replace("To", "TTOO"));
                bw.newLine();
            }
        }

        Files.delete(path);
        Files.move(tempPath, path);
        long end = System.currentTimeMillis();
        System.out.println("File update time: " + (end - start) + " ms");
    }
}
