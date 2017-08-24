import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class similarity {

    public static class similarityMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: m1,m2 \t cooccurance
            // output: (m1, m1:cooccur1, m2:cooccur2, ...)
            String[] values = value.toString().trim().split("\t");
            //context.write(new Text(values[0]), new Text(values[1]));
            String[] movies = values[0].split(",");
            context.write(new Text(movies[0]), new Text(movies[1] + ":" + values[1]));
        }
    }

    public static class similarityReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            // output: (m1, m1:cooccur1/N, m2:cooccur2/N, ...)
            int sum = 0;
            Map<String, Integer> movieCooccurMap = new HashMap<>();
            while(values.iterator().hasNext()) {
                // context.write(key, values.iterator().next());
                String[] item = values.iterator().next().toString().split(":");
                int cooccur = Integer.parseInt(item[1]);
                movieCooccurMap.put(item[0], cooccur);
                sum += cooccur;
            }
            for(Map.Entry<String, Integer> entry : movieCooccurMap.entrySet()) {
                String m2 = entry.getKey();
                double cooccurSim = entry.getValue()/((double)sum);
                context.write(new Text(m2), new Text(key.toString() + "=" + Double.toString(cooccurSim)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(similarityMapper.class);
        job.setReducerClass(similarityReducer.class);

        job.setJarByClass(similarity.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
