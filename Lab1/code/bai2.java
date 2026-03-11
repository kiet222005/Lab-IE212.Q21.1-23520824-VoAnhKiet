import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai2 {

    // Mapper đọc ratings
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieId = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            String movie = parts[1].trim();
            String rating = parts[2].trim();

            movieId.set(movie);
            ratingValue.set("R:" + rating);

            context.write(movieId, ratingValue);
        }
    }

    // Mapper đọc movies
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieId = new Text();
        private Text genreValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // split 3 phần để tránh lỗi dấu phẩy trong title
            String[] parts = line.split(",", 3);
            if (parts.length < 3) return;

            String movie = parts[0].trim();
            String genres = parts[2].trim();

            movieId.set(movie);
            genreValue.set("G:" + genres);

            context.write(movieId, genreValue);
        }
    }

    // Reducer
    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {

        // Genre → [sum, count]
        private Map<String, double[]> genreStats = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String genres = "";
            double sum = 0;
            int count = 0;

            for (Text val : values) {

                String v = val.toString();

                if (v.startsWith("R:")) {

                    try {
                        double rating = Double.parseDouble(v.substring(2));
                        sum += rating;
                        count++;
                    } catch (Exception e) {
                        // ignore lỗi
                    }

                } else if (v.startsWith("G:")) {

                    genres = v.substring(2);
                }
            }

            // bỏ qua nếu không có rating hoặc genre
            if (count == 0 || genres.isEmpty()) return;

            // mỗi phim có thể có nhiều genre
            String[] genreList = genres.split("\\|");

            for (String g : genreList) {

                g = g.trim();

                genreStats.computeIfAbsent(g, k -> new double[]{0.0, 0.0});

                genreStats.get(g)[0] += sum;
                genreStats.get(g)[1] += count;
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Map.Entry<String, double[]> entry : genreStats.entrySet()) {

                String genre = entry.getKey();

                double totalSum = entry.getValue()[0];
                int totalCount = (int) entry.getValue()[1];

                double avg = totalSum / totalCount;

                context.write(
                        new Text(genre + " "),
                        new Text(String.format("Avg: %.2f,  Count: %d", avg, totalCount))
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: bai2 <ratings input> <movies input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Genre Rating Analysis");

        job.setJarByClass(bai2.class);

        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
                job,
                new Path(args[0]),
                TextInputFormat.class,
                RatingMapper.class
        );

        MultipleInputs.addInputPath(
                job,
                new Path(args[1]),
                TextInputFormat.class,
                MovieMapper.class
        );

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}