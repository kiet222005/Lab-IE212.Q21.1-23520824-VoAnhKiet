import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai3 {

    // JOB 1 - Join ratings + users theo UserID

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            String userId = parts[0].trim();
            String movieId = parts[1].trim();
            String rating = parts[2].trim();

            context.write(new Text(userId),
                    new Text("R:" + movieId + ":" + rating));
        }
    }

    public static class UserMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 2) return;

            String userId = parts[0].trim();
            String gender = parts[1].trim();

            context.write(new Text(userId),
                    new Text("G:" + gender));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String gender = null;
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {

                String v = val.toString();

                if (v.startsWith("G:"))
                    gender = v.substring(2);

                else if (v.startsWith("R:"))
                    ratings.add(v.substring(2));
            }

            if (gender == null) return;

            for (String r : ratings) {

                String[] parts = r.split(":");
                if (parts.length < 2) continue;

                String movieId = parts[0];
                String rating = parts[1];

                context.write(new Text(movieId),
                        new Text("GR:" + gender + ":" + rating));
            }
        }
    }

    // JOB 2 - Join movies + tính average theo gender

    public static class GenderRatingMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length < 2) return;

            String movieId = parts[0].trim();
            String genderRating = parts[1].trim();

            context.write(new Text(movieId), new Text(genderRating));
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",", 3);
            if (parts.length < 2) return;

            String movieId = parts[0].trim();
            String title = parts[1].trim();

            context.write(new Text(movieId),
                    new Text("T:" + title));
        }
    }

    public static class AggReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String title = null;

            double maleSum = 0;
            double femaleSum = 0;

            int maleCount = 0;
            int femaleCount = 0;

            for (Text val : values) {

                String v = val.toString();

                if (v.startsWith("T:")) {

                    title = v.substring(2);

                } else if (v.startsWith("GR:")) {

                    String[] parts = v.split(":");
                    if (parts.length < 3) continue;

                    String gender = parts[1];

                    double rating;
                    try {
                        rating = Double.parseDouble(parts[2]);
                    } catch (Exception e) {
                        continue;
                    }

                    if (gender.equals("M")) {
                        maleSum += rating;
                        maleCount++;
                    } else if (gender.equals("F")) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                }
            }

            if (title == null) return;

            double maleAvg = maleCount > 0 ? maleSum / maleCount : 0;
            double femaleAvg = femaleCount > 0 ? femaleSum / femaleCount : 0;

            String result = String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg);

            context.write(new Text(title + ":"), new Text(result));
        }
    }

    // MAIN

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println("Usage: bai3 <ratings> <users> <movies> <tmp> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        String ratings = args[0];
        String users = args[1];
        String movies = args[2];
        String tmp = args[3];
        String output = args[4];

        // ---------------- JOB 1 ----------------

        Job job1 = Job.getInstance(conf, "Join Ratings + Users");

        job1.setJarByClass(bai3.class);

        job1.setReducerClass(JoinReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(ratings),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job1, new Path(users),
                TextInputFormat.class, UserMapper.class);

        FileOutputFormat.setOutputPath(job1, new Path(tmp));

        if (!job1.waitForCompletion(true))
            System.exit(1);

        // ---------------- JOB 2 ----------------

        Job job2 = Job.getInstance(conf, "Aggregate Ratings by Gender");

        job2.setJarByClass(bai3.class);

        job2.setReducerClass(AggReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(tmp),
                TextInputFormat.class, GenderRatingMapper.class);

        MultipleInputs.addInputPath(job2, new Path(movies),
                TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job2, new Path(output));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}