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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class normalizeRating {
	public static class baselineMapper extends Mapper<LongWritable, Text, Text, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: (userID,movieID, global_baseline)
			
			String[] outputList = value.toString().trim().split("\t");
            
			context.write(new Text(outputList[0]), new Text("globalAVG=" + outputList[1]));
		}
	}
    public static class itemBiasMapper extends Mapper<LongWritable, Text, Text, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: (movieID, [userID:score, ..., AVG:avg, SUM:sum, COUNT: count])
		    // output: (k, v) = (userID,movieID, itemAVG = avg)
			

            String[] movieIDScoreList = value.toString().trim().split("\t");
			String movie = movieIDScoreList[0];
            String[] movieID_Score = movieIDScoreList[1].split(",");
			if (movieID_Score.length > 2){
				String itemAVG = movieID_Score[movieID_Score.length - 3].split(":")[1];
			    for (int i = 0; i < movieID_Score.length - 3; i++) {
				    String item = movieID_Score[i];
                    String user = item.split(":")[0];
				    context.write(new Text(user + "," + movie), new Text("itemAVG=" + itemAVG));
			    }
			}
		}
	}
    public static class userBiasMapper extends Mapper<LongWritable, Text, Text, Text> {
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: (userID, [movieID:score, ..., AVG:avg, SUM:sum, COUNT: count])
		    // output: (k, v) = (userID,movieID, userAVG = avg)
			
			String[] userIDScoreList = value.toString().trim().split("\t");
			String user = userIDScoreList[0];
            String[] userID_Score = userIDScoreList[1].split(",");
			if (userID_Score.length > 2) {
                String userAVG = userID_Score[userID_Score.length - 3].split(":")[1];
			    for (int i = 0; i < userID_Score.length - 3; i++) {
				    String item = userID_Score[i];
                    String movie = item.split(":")[0];
					String raw = item.split(":")[1];
				    context.write(new Text(user + "," + movie), new Text("userAVG=" + userAVG));
					context.write(new Text(user + "," + movie), new Text("RAW=" + raw));
			    }
			}
		}
	}
	public static class normalizeReducer extends Reducer<Text, Text, Text, Text> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double itemAVG = 0., userAVG = 0., globalAVG = 0., raw = 0.;
			while (values.iterator().hasNext()) {
				String item = values.iterator().next().toString();
				//context.write(key, new Text(item));
				String[] value = item.split("=");
				//context.write(key, new Text(value[0]));
				if (value[0].equals("itemAVG")) {
					itemAVG = Double.parseDouble(value[1]);
					//context.write(key, new Text(Double.toString(itemAVG)));
				} else if (value[0].equals("userAVG")) {
					userAVG = Double.parseDouble(value[1]);
					//context.write(key, new Text(Double.toString(userAVG)));
				} else if (value[0].equals("globalAVG")) {
					globalAVG = Double.parseDouble(value[1]);
					//context.write(key, new Text(Double.toString(baseline)));
				} else if (value[0].equals("RAW")) {
					raw = Double.parseDouble(value[1]);
					//context.write(key, new Text(Double.toString(raw)));
				}
			}
			double baseline = itemAVG + userAVG - globalAVG;
			double normalized = raw - itemAVG - userAVG + globalAVG;
		    context.write(key, new Text("normalized=" + Double.toString(normalized)
	        + ",baseline=" + Double.toString(baseline)));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(normalizeRating.class);
        
		ChainMapper.addMapper(job, baselineMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, itemBiasMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, userBiasMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(baselineMapper.class);
		job.setMapperClass(itemBiasMapper.class);
		job.setMapperClass(userBiasMapper.class);
		job.setReducerClass(normalizeReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, baselineMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, itemBiasMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, userBiasMapper.class);
		TextOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}

}
