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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class matmul {
	public static class simMatMapper extends Mapper<LongWritable, Text, Text, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input & output: m2 \t m1=similarity
			
			String[] output = value.toString().trim().split("\t");
            
			context.write(new Text(output[0]), new Text(output[1]));
		}
	}
    public static class scoreMapper extends Mapper<LongWritable, Text, Text, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: userID,movieID \t normalized = score1, baseline = score2
		    // output: movieID, userID:score1
			String[] line = value.toString().trim().split("\t");
			String[] user_movie = line[0].split(",");
			String score = line[1].split(",")[0].split("=")[1];
			context.write(new Text(user_movie[1]), new Text(user_movie[0] + ":" + score));
		}
	}
    
	public static class matmulReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// input: m2, m1 = similarity
			// input: m2, user : score
			// output: user:m1 = score * similarity
			Map<String, Double> simMap = new HashMap<>();
			Map<String, Double> scoreMap = new HashMap<>();
			while (values.iterator().hasNext()) {
				String item = values.iterator().next().toString();
				// context.write(key, new Text(item));
				if (item.contains("=")) {
					String[] movieSim = item.split("=");
					simMap.put(movieSim[0], Double.parseDouble(movieSim[1]));
				} else {
					String[] userScore = item.split(":");
					scoreMap.put(userScore[0], Double.parseDouble(userScore[1]));
				}
			}
			for (Map.Entry<String, Double> entry : simMap.entrySet()) {
				String movie = entry.getKey();
				double similarity = entry.getValue();
				for (Map.Entry<String, Double> entry2 : scoreMap.entrySet()) {
					String user = entry2.getKey();
					double score = entry2.getValue();
					context.write(new Text(user + "," + movie), new DoubleWritable(score * similarity));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(matmul.class);
        
		ChainMapper.addMapper(job, simMatMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, scoreMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
        

		job.setMapperClass(simMatMapper.class);
		job.setMapperClass(scoreMapper.class);
		
		job.setReducerClass(matmulReducer.class);

		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, simMatMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, scoreMapper.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}

}
