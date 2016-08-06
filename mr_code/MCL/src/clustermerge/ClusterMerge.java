package clustermerge;

import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class ClusterMerge extends Configured implements Tool {
	
	private static final String INTERMEDIATE_PATH = "/clustermerge";
	private enum Collisions {vertex1, vertex2};


	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		for (int i=0; ; i++) {
		
			Job job1 = Job.getInstance(conf, "ClusterMerge 1");
			job1.setJarByClass(ClusterMerge.class);
			job1.setMapperClass(FindCollisions.class);
			job1.setReducerClass(SelectCollision.class);
			job1.setNumReduceTasks(10);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			if (i==0) {
				FileInputFormat.addInputPath(job1, new Path(args[0]));
			} else {
				FileInputFormat.addInputPath(job1, new Path(args[1]+INTERMEDIATE_PATH+(i-1)+"b/"));
			}
			FileOutputFormat.setOutputPath(job1, new Path(args[1]+INTERMEDIATE_PATH+i+"a/"));
			job1.waitForCompletion(true);

			long vertex1 = job1.getCounters().findCounter(Collisions.vertex1).getValue();
			long vertex2 = job1.getCounters().findCounter(Collisions.vertex2).getValue();
			if (vertex1 == 0 && vertex2 == 0) {
				break;
			}

			conf.set("collision", vertex1+","+vertex2);
			
			Job job2 = Job.getInstance(conf, "ClusterMerge 2");
			job2.setJarByClass(ClusterMerge.class);
			job2.setMapperClass(RemoveCollision.class);
			job2.setNumReduceTasks(0);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(args[1]+INTERMEDIATE_PATH+i+"a/"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+INTERMEDIATE_PATH+i+"b/"));
			job2.waitForCompletion(true);
			
			conf.set("collsion", "none");
		}
		return 0;
	}

	private static class FindCollisions extends Mapper<LongWritable, Text, Text, Text> {

		private static boolean foundCollision = false;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws 
		               java.io.IOException, InterruptedException {
			String inputString = value.toString();
			String stringVal = inputString.split("\t")[1];
			if (!foundCollision) {
				int firstPos = stringVal.indexOf(',');
				if (firstPos != -1) {
					String firstZone = stringVal.substring(0, firstPos);
					
					String secondZone;
					int secondPos = stringVal.indexOf(',', firstPos+1);
					if (secondPos != -1) {
						secondZone = stringVal.substring(firstPos+1, secondPos);
					} else {
						secondZone = stringVal.substring(firstPos+1);
					}
					foundCollision = true;
					context.write(new Text("COLLISION"), new Text(firstZone + "," + secondZone));
				}
			}
			context.write(new Text(inputString.split("\t")[0]), new Text(stringVal));
		}
	}

	private static class SelectCollision extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws 
		               java.io.IOException, InterruptedException {
			if (key.toString().equals("COLLISION")) {
				String stringVals[] = values.iterator().next().toString().split(",");
				context.getCounter(Collisions.vertex1).setValue(Integer.parseInt(
				                                                 stringVals[0]));
				context.getCounter(Collisions.vertex2).setValue(Integer.parseInt(
				                                                 stringVals[1]));
			} else {
				for (Text val : values) {
					context.write(key, val);
				}
			}
		}
	}

	private static class RemoveCollision extends Mapper<LongWritable, Text, Text, Text> {
		private String toZone;
		private String fromZone;
		
		@Override
		protected void setup(Context context) {
			String collisions[] = context.getConfiguration().get("collision").split(",");
			fromZone = collisions[0];
			toZone = collisions[1];
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws 
		               java.io.IOException, InterruptedException {
			String inputSplit[] = value.toString().split("\t");
			HashSet<String> zones = new HashSet<String>(inputSplit[1].split(",").length);
			for (String zone : inputSplit[1].split(",")) {
				if (zone.equals(fromZone)) {
					zones.add(toZone);
				} else {
					zones.add(zone);
				}
			}
			String outputString = "";
			for (String zone : zones) {
				outputString += (zone + ",");
			}
			if (outputString.length() > 0) {
				outputString = outputString.substring(0, outputString.length()-1);
				context.write(new Text(inputSplit[0]), new Text(outputString));
			}
		}
	}
}
