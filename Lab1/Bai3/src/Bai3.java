import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, String> userGenderMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].getPath().split("/")[cacheFiles[0].getPath().split("/").length-1]));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    if (fields.length >= 2) userGenderMap.put(fields[0].trim(), fields[1].trim());
                }
                reader.close();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String userId = fields[0].trim();
                String movieId = fields[1].trim();
                String rating = fields[2].trim();
                String gender = userGenderMap.getOrDefault(userId, "U");
                context.write(new Text(movieId), new Text(gender + ":" + rating));
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> movieTitleMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 1) {
                BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[1].getPath().split("/")[cacheFiles[1].getPath().split("/").length-1]));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",", 2);
                    if (fields.length >= 2) movieTitleMap.put(fields[0].trim(), fields[1].split(",")[0].trim());
                }
                reader.close();
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String gender = parts[0];
                double rating = Double.parseDouble(parts[1]);

                if (gender.equals("M")) {
                    maleSum += rating; maleCount++;
                } else if (gender.equals("F")) {
                    femaleSum += rating; femaleCount++;
                }
            }

            String movieTitle = movieTitleMap.getOrDefault(key.toString(), "Unknown");
            String result = String.format("Male: %.2f, Female: %.2f", 
                            (maleCount > 0 ? maleSum/maleCount : 0.0), 
                            (femaleCount > 0 ? femaleSum/femaleCount : 0.0));
            context.write(new Text(movieTitle), new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Ratings by Gender");
        job.setJarByClass(Bai3.class);
        
        job.addCacheFile(new Path(args[0]).toUri()); 
        job.addCacheFile(new Path(args[1]).toUri()); 

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[2])); 
        FileInputFormat.addInputPath(job, new Path(args[3])); 
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}