import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                try {
                    double rating = Double.parseDouble(parts[2]);
                    context.write(new Text(parts[1].trim()), new Text("R:" + rating));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid rating: " + value.toString());
                }
            }
        }
    }

    public static class MoviesMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 3);
            if (parts.length >= 3) {
                String movieID = parts[0].trim();
                String genres = parts[2].trim();
                context.write(new Text(movieID), new Text("M:" + genres));
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Double> genreTotalRatings = new HashMap<String, Double>();
        private Map<String, Integer> genreCounts = new HashMap<String, Integer>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalRating = 0.0;
            int count = 0;
            String[] genres = new String[0];

            for (Text val : values) {
                String[] parts = val.toString().split(":", 2);
                if (parts[0].equals("R")) {
                    totalRating += Double.parseDouble(parts[1]);
                    count++;
                } else if (parts[0].equals("M")) {
                    genres = parts[1].split("\\|");
                }
            }

            if (count > 0) {
                for (String genre : genres) {
                    genre = genre.trim();
                    if (!genreTotalRatings.containsKey(genre)) {
                        genreTotalRatings.put(genre, 0.0);
                    }
                    if (!genreCounts.containsKey(genre)) {
                        genreCounts.put(genre, 0);
                    }
                    genreTotalRatings.put(genre, genreTotalRatings.get(genre) + totalRating);
                    genreCounts.put(genre, genreCounts.get(genre) + count);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Double> entry : genreTotalRatings.entrySet()) {
                String genre = entry.getKey();
                double avgRating = Math.round((entry.getValue() / genreCounts.get(genre)) * 100.0) / 100.0;
                int totalRatings = genreCounts.get(genre);
                context.write(new Text(genre), new Text("Avg: " + String.format("%.2f", avgRating) + ", Count: " + totalRatings + ""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Ratings Calculation");
        job.setJarByClass(Bai2.class);
        job.setReducerClass(GenreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, MoviesMapper.class);

        Path outputPath = new Path(args[3]);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
