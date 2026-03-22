package bigdata;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CleanReview extends EvalFunc<String> {
    private Set<String> stopwords = new HashSet<>();

    // Constructor nhận đường dẫn file stopwords từ Pig script
    public CleanReview(String stopwordPath) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(stopwordPath));
            String line;
            while ((line = reader.readLine()) != null) {
                stopwords.add(line.trim().toLowerCase());
            }
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException("Lỗi đọc file stopword: " + e.getMessage());
        }
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }

        // Bước 1: Lấy chuỗi review và chuyển thành chữ thường
        String review = ((String) input.get(0)).toLowerCase();

        // Bước 2: Tách từ theo khoảng trắng
        String[] words = review.split("\\s+");

        // Bước 3: Lọc stopword
        StringBuilder result = new StringBuilder();
        for (String word : words) {
            if (!stopwords.contains(word)) {
                result.append(word).append(" ");
            }
        }

        return result.toString().trim();
    }
}
