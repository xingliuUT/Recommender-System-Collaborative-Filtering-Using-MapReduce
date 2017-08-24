import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class cooccurance {
	public static class cooccuranceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: (userID, [movieID:score, ..., AVG:avg, SUM:sum, COUNT: count])
		    // output: (k, v) = (movie1:movie2, 1)
			
			String[] userIDScoreList = value.toString().trim().split("\t");
            String[] movieID_Score = userIDScoreList[1].split(",");
			if (movieID_Score.length > 2) {
			    for (int i = 0; i < movieID_Score.length - 3; i++) {
				    String item1 = movieID_Score[i];
                    String movie1 = item1.split(":")[0];
					for (int j = 0; j < movieID_Score.length - 3; j++) {
				        String item2 = movieID_Score[j];
                        String movie2 = item2.split(":")[0];
						context.write(new Text(movie1 + "," + movie2), new IntWritable(1));
					}
			    }
			}
		}
	}

	public static class cooccuranceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// output: (k, v) = (movie1:movie2, cooccur.)
			int sum = 0;
			while (values.iterator().hasNext()) {
				String value = values.iterator().next().toString();
				sum += Integer.parseInt(value);
			}
            context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(cooccuranceMapper.class);
		job.setReducerClass(cooccuranceReducer.class);

		job.setJarByClass(cooccurance.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
