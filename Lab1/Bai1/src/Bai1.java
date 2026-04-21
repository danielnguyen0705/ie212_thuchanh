import java.io.IOException;
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

public class Bai1 {

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
            String[] parts = value.toString().split(",", 2);
            if (parts.length >= 2) {
                String movieID = parts[0].trim();
                String movieTitle = parts[1].split(",")[0].trim();
                context.write(new Text(movieID), new Text("M:" + movieTitle));
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
    // Khai báo biến lớp để theo dõi phim "vô địch" 
    private String maxMovie = "";
    private double maxRating = -1.0;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalRating = 0.0;
        int count = 0;
        String movieTitle = "Unknown Movie";

        for (Text val : values) {
            String[] parts = val.toString().split(":", 2);
            if (parts[0].equals("R")) {
                totalRating += Double.parseDouble(parts[1]);
                count++;
            } else if (parts[0].equals("M")) {
                movieTitle = parts[1]; // Lấy tên phim từ tag M:
            }
        }

        if (count > 0) {
            double avgRating = totalRating / count;
            
            // Xuất kết quả từng phim như bình thường 
            String result = String.format("Average rating: %.1f (Total ratings: %d)", avgRating, count);
            context.write(new Text(movieTitle), new Text(result));

            // Cập nhật phim có điểm cao nhất 
            if (count >= 5 && avgRating > maxRating) {
                maxRating = avgRating;
                maxMovie = movieTitle;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!maxMovie.isEmpty()) {
            context.write(new Text(maxMovie), new Text("is the highest rated movie with an average rating of " 
                + maxRating + " among movies with at least 5 ratings."));
        }
    }
}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Ratings Calculation");
        job.setJarByClass(Bai1.class);
        job.setReducerClass(RatingReducer.class);
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
