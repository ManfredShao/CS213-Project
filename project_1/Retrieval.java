import java.io.BufferedReader;
import java.io.FileReader;

public class Retrieval {
    public static void main(String[] args) throws Exception {
        String keyword = "To";
        String filePath = "movies_large.csv";

        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        long start = System.currentTimeMillis();

        while ((line = br.readLine()) != null) {
            if (line.toLowerCase().contains(keyword.toLowerCase())) {
                System.out.println(line);
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("Search time: " + (end - start) + " ms");
        br.close();
    }
}
