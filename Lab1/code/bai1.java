import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai1 {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length >= 3) {
                try {
                    movieIdKey.set(parts[1].trim());
                    ratingValue.set("Rate:" + parts[2].trim());
                    context.write(movieIdKey, ratingValue);
                } catch (Exception e) {}
            }
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text movieNameValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length >= 2) {
                movieIdKey.set(parts[0].trim());
                movieNameValue.set("Movie:" + parts[1].trim());
                context.write(movieIdKey, movieNameValue);
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private String maxMovie = "";
        private double maxRating = 0.0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            String movieName = "";

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("Rate:")) {
                    sum += Double.parseDouble(value.replace("Rate:", ""));
                    count++;
                } else if (value.startsWith("Movie:")) {
                    movieName = value.replace("Movie:", "");
                }
            }

            if (count > 0 && !movieName.isEmpty()) {
                double avg = sum / count;
                
                context.write(new Text(movieName), new Text(String.format("Average Rating: %.2f (Total Ratings: %d)", avg, count)));

                if (count >= 5 && avg > maxRating) {
                    maxRating = avg;
                    maxMovie = movieName;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(maxMovie), new Text(String.format("is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings", maxRating)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis");
        job.setJarByClass(bai1.class);
        job.setReducerClass(RatingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}