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

public class preProcessBaseline {
	public static class baselineMapper extends Mapper<LongWritable, Text, Text, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: (movieID, [userID:score, ..., AVG:avg, SUM:sum, COUNT: count])
		    // output: (k, v) = (BASELINE, sum / count)
			// StringBuilder sum = new StringBuilder();
			// StringBuilder count = new StringBuilder();
			String[] movieIDScoreList = value.toString().trim().split("\t");
            String[] movieID_Score = movieIDScoreList[1].split(",");
			String sumsum = movieID_Score[movieID_Score.length - 2];
			String countcount = movieID_Score[movieID_Score.length - 1];
			String sum = sumsum.split(":")[1];
			String count = countcount.split(":")[1];
			// for (String item : movieID_Score) {
			// 	String[] thisItem = item.split(":");
			// 	if (thisItem[0] == "SUM") {
            //         sum.append(thisItem[1]);
			// 	} else if (thisItem[0] == "COUNT") {
			// 		count.append(thisItem[1]);
			// 	}
			// }
			context.write(new Text("BASELINE"), new Text(sum + ":" + count));
		}
	}

	public static class baselineReducer extends Reducer<Text, Text, Text, Text> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// output: (k, v) = (GLOBAL, mu)
			double globalSum = 0.;
			int globalCount = 0;
			while (values.iterator().hasNext()) {
				String value = values.iterator().next().toString();
				String[] sum_count = value.trim().split(":");
				globalSum += Double.parseDouble(sum_count[0]);
				globalCount += Integer.parseInt(sum_count[1]);
			}
            double globalAvg = globalSum / globalCount;
            context.write(new Text("GLOBAL"), new Text(Double.toString(globalAvg)));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(baselineMapper.class);
		job.setReducerClass(baselineReducer.class);

		job.setJarByClass(preProcessBaseline.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
