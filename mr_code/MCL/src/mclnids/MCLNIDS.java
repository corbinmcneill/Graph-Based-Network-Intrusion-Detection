package mclnids;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import clustermerge.ClusterMerge;
import mcl.MCL;

import org.apache.hadoop.fs.Path;

public class MCLNIDS extends Configured implements Tool {

	private static final String INTERMEDIATE_PATH_1 = "mcl/";
	private static final String INTERMEDIATE_PATH_2 = "conversion/";
	private static final String FINAL_PATH = "cluster_merge/";

	public static int main(String[] args) throws Exception {
        // Parse the command line variables:
        // args[0] = input location, 
        // args[1] = output location <- IGNORED
        // args[2] = number of runs
        // args[3] = number of vertices
        // args[4] = number of reducers (I set this to the number of my processor's cores)
        // args[5] = number of cycles (for expansion & inflation)
        // args[6] = inflation parameter (the power that each col. is raised to)

		String custromargs[] = args;

		custromargs[1] = INTERMEDIATE_PATH_1;
		ToolRunner.run(new MCL(), custromargs);

		ToolRunner.run(
		    new MCLNIDS(),
		    new String[] {INTERMEDIATE_PATH_1+"MR5-run"+(Integer.parseInt(args[2])-1),
		                  INTERMEDIATE_PATH_2}
		);

		ToolRunner.run(new ClusterMerge(), new String[] {INTERMEDIATE_PATH_2, FINAL_PATH});

        return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		Job job1 = Job.getInstance(conf, "Reformatting");
		job1.setJarByClass(MCLNIDS.class);
		job1.setMapperClass(ReformatMapper.class);
		job1.setReducerClass(ReformatReducer.class);
		job1.setNumReduceTasks(10);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		return 0;
	}

	private static class ReformatMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		@Override
		protected void map(LongWritable key, Text value, Context context) throws 
		               java.io.IOException, InterruptedException {
			String inputSplit[] = value.toString().split("\t");
			String clusterID = inputSplit[0].split(":")[1];
			for (String v : inputSplit[1].split(", ")) {
				context.write(new Text(v), new Text(clusterID));
			}
		}
	}

    private class ReformatReducer extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) 
		               throws java.io.IOException, InterruptedException {
			String outputValue = "";
			for (Text val : values) {
				outputValue += val.toString() + ",";
			}
			if (outputValue.length() > 0) {
				outputValue = outputValue.substring(0,outputValue.length()-1);
				context.write(key, new Text(outputValue));
			}
		}
	}
}
