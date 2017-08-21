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

import java.io.IOException;

public class preProcessByItem {
	public static class byItemMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: userID, movieID, score
		    // output: (k, v) = (movieID, userID:score)
			String[] userID_movieID_rawScore = value.toString().trim().split(",");
			String userID = userID_movieID_rawScore[0];
			int movieID = Integer.parseInt(userID_movieID_rawScore[1]);
			String rawScore = userID_movieID_rawScore[2];
			context.write(new IntWritable(movieID), new Text(userID + ":" + rawScore));
		}
	}

	public static class byItemReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// output: (k, v) = (movieID, user1:score1, user2:score2, ...)
			StringBuffer outValue = new StringBuffer();
			while (values.iterator().hasNext()) {
				outValue.append(values.iterator().next() + ",");
			}

            context.write(key, new Text(outValue.toString().substring(0, outValue.length() - 1)));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(byItemMapper.class);
		job.setReducerClass(byItemReducer.class);

		job.setJarByClass(preProcessByItem.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
