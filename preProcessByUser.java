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

import java.io.IOException;

public class preProcessByUser {
	public static class byUserMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input: userID, movieID, score
		    // output: (k, v) = (userID, movieID:score)
			String[] userID_movieID_rawScore = value.toString().trim().split(",");
			int userID = Integer.parseInt(userID_movieID_rawScore[0]);
			String movieID = userID_movieID_rawScore[1];
			String rawScore = userID_movieID_rawScore[2];
			context.write(new IntWritable(userID), new Text(movieID + ":" + rawScore));
		}
	}

	public static class byUserReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// output: (k, v) = (userID, movie1:score1, movie2:score2, ...)
			StringBuffer outValue = new StringBuffer();
			double userSum = 0.;
			int movieNum = 0;
			while(values.iterator().hasNext()) {
				String value = values.iterator().next().toString();
				String[] movieID_rawScore = value.trim().split(":");
				userSum += Double.parseDouble(movieID_rawScore[1]);
				movieNum += 1;
				outValue.append(value + ",");
			}
			outValue.append("AVG:" + Double.toString(userSum / movieNum));
            context.write(key, new Text(outValue.toString()));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(byUserMapper.class);
		job.setReducerClass(byUserReducer.class);

		job.setJarByClass(preProcessByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
