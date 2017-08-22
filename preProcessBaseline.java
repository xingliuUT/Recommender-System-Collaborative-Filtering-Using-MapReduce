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

public class preProcessBaseline {
	public static class baselineMapper extends Mapper<LongWritable, Text, Text, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: (movieID, [userID:score, ..., AVG:avg, SUM:sum, COUNT: count])
		    // output: (k, v) = (BASELINE, sum : count : movieID + user1 + user2 +...)
			
			String[] movieIDScoreList = value.toString().trim().split("\t");
            String[] movieID_Score = movieIDScoreList[1].split(",");
			StringBuffer userList = new StringBuffer();
			for (int i = 0; i < movieID_Score.length - 3; i++) {
				String item = movieID_Score[i];
                userList.append("=" + item.split(":")[0]);
			}
			String sumsum = movieID_Score[movieID_Score.length - 2];
			String countcount = movieID_Score[movieID_Score.length - 1];
			String sum = sumsum.split(":")[1];
			String count = countcount.split(":")[1];
			
			context.write(new Text("BASELINE"), new Text(sum + ":" + count + ":" + 
			movieIDScoreList[0] + userList.toString()));
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
			Map<String,ArrayList<String>> userID_movieIDMap = new HashMap<>();
			while (values.iterator().hasNext()) {
				String value = values.iterator().next().toString();
				String[] sum_count = value.trim().split(":");
				globalSum += Double.parseDouble(sum_count[0]);
				globalCount += Integer.parseInt(sum_count[1]);
				String[] userID_movieIDList = sum_count[2].trim().split("=");
				ArrayList<String> movieList = new ArrayList<>();
				for(int i = 1; i < userID_movieIDList.length; i++) {
					movieList.add(userID_movieIDList[i]);
				}
                userID_movieIDMap.put(userID_movieIDList[0], movieList);
			}
            double globalAvg = globalSum / globalCount;
			for(Map.Entry<String,ArrayList<String>> entry: userID_movieIDMap.entrySet()) {
				String movieID = entry.getKey();
				ArrayList<String> userList = entry.getValue();
				for (String user : userList){
                    context.write(new Text(user + "," + movieID), new Text(Double.toString(globalAvg)));
				}
			}
            
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
