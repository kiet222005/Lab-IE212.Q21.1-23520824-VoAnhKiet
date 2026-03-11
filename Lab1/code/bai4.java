import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai4 {

    private static final String[] AGE_GROUPS = {"0-18", "18-35", "35-50", "50+"};

    private static String getAgeGroup(int age) {
        if (age <= 18) return "0-18";
        if (age <= 35) return "18-35";
        if (age <= 50) return "35-50";
        return "50+";
    }

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
            if (parts.length < 3) return;

            String userId = parts[0].trim();
            String ageStr = parts[2].trim();

            context.write(new Text(userId),
                    new Text("A:" + ageStr));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String ageGroup = null;
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {

                String v = val.toString();

                if (v.startsWith("A:")) {
                    try {
                        int age = Integer.parseInt(v.substring(2));
                        ageGroup = getAgeGroup(age);
                    } catch (Exception e) {}
                }

                else if (v.startsWith("R:")) {
                    ratings.add(v.substring(2));
                }
            }

            if (ageGroup == null) return;

            for (String r : ratings) {

                String[] parts = r.split(":");
                if (parts.length < 2) continue;

                String movieId = parts[0];
                String rating = parts[1];

                context.write(new Text(movieId),
                        new Text("AR:" + ageGroup + ":" + rating));
            }
        }
    }

    // JOB 2 - Join movies + tính average theo age group

    public static class AgeRatingMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length < 2) return;

            context.write(new Text(parts[0]), new Text(parts[1]));
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

            Map<String, double[]> stats = new HashMap<>();

            for (Text val : values) {

                String v = val.toString();

                if (v.startsWith("T:")) {
                    title = v.substring(2);
                }

                else if (v.startsWith("AR:")) {

                    String[] parts = v.split(":", 3);
                    if (parts.length < 3) continue;

                    String ageGroup = parts[1];

                    double rating;
                    try {
                        rating = Double.parseDouble(parts[2]);
                    } catch (Exception e) {
                        continue;
                    }

                    stats.computeIfAbsent(ageGroup, k -> new double[]{0,0});

                    stats.get(ageGroup)[0] += rating;
                    stats.get(ageGroup)[1] += 1;
                }
            }

            if (title == null) return;

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < AGE_GROUPS.length; i++) {

                String group = AGE_GROUPS[i];

                if (i > 0) sb.append(", ");

                sb.append(group).append(": ");

                double[] d = stats.get(group);

                if (d == null || d[1] == 0)
                    sb.append("NA");
                else
                    sb.append(String.format("%.2f", d[0]/d[1]));
            }

            context.write(new Text(title + ":"), new Text("[" + sb.toString() + "]"));
        }
    }

    // MAIN

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println("Usage: bai4 <ratings> <users> <movies> <tmp> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        String ratings = args[0];
        String users = args[1];
        String movies = args[2];
        String tmp = args[3];
        String output = args[4];

        // JOB 1

        Job job1 = Job.getInstance(conf, "Join Ratings + Users");

        job1.setJarByClass(bai4.class);

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

        // JOB 2

        Job job2 = Job.getInstance(conf, "Aggregate Ratings by Age Group");

        job2.setJarByClass(bai4.class);

        job2.setReducerClass(AggReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(tmp),
                TextInputFormat.class, AgeRatingMapper.class);

        MultipleInputs.addInputPath(job2, new Path(movies),
                TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job2, new Path(output));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}