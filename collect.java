import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.HashMap;
import java.util.Map;

import java.io.IOException;

public class collect{
	public static class collectMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input & output: (userID, movieID), score
		    
			String[] line = value.toString().trim().split("\t");
			double score = Double.parseDouble(line[1]);
			context.write(new Text(line[0]), new DoubleWritable(score));
		}
	}

	public static class collectReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			// output: (user, movie) = sum(score)
			
			double sum = 0.;
			while(values.iterator().hasNext()) {
				String value = values.iterator().next().toString();
				sum += Double.parseDouble(value);
			}
            context.write(key, new DoubleWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(collectMapper.class);
		job.setReducerClass(collectReducer.class);

		job.setJarByClass(collect.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
