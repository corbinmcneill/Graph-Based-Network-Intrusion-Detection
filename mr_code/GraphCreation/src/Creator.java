/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import SimilarityMeasure.EuclideanSquared;
import SimilarityMeasure.SimilarityMeasure;


public class Creator {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		ArrayList<String> lines = new ArrayList<String>();
		
		@Override
		public void setup(Context context) throws IOException {
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				URI graphUri = context.getCacheFiles()[0];
				if (graphUri != null) {
					Path pt = new Path(graphUri.toString());
					FileSystem fs = FileSystem.get(new Configuration());
					InputStreamReader isr = new InputStreamReader(fs.open(pt));
					BufferedReader br = new BufferedReader(isr);
					String line;
					while (true) {
						line = br.readLine();
						if (line == null) {break;}
						lines.add(line);
					}
					br.close();
					isr.close();
				}
			}
		}
	  
		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			int i = 0;
			while (val.charAt(i) != ' ') {
			  i++;
			}
			int number;
			number = Integer.parseInt(val.toString().substring(0, i));
			for (int j = number+1; j<lines.size(); j++) {
				context.write(val, new Text(lines.get(j)));
			}
		}
	  
	}

	public static class ReduceClass extends Reducer<Text, Text, IntWritable, Text> {
		
		SimilarityMeasure sm = new EuclideanSquared();

  		@Override 
  		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
  			
  			StringTokenizer splitA1 = new StringTokenizer(key.toString());
  			int a = Integer.parseInt(splitA1.nextToken());
  			String[] stringA = splitA1.nextToken().split(" |,");
  			
  			for (Text val : vals) {
  				
  				StringTokenizer splitB1 = new StringTokenizer(val.toString());
  				int b = Integer.parseInt(splitB1.nextToken());
  				String[] stringB = splitB1.nextToken().split(" |,");  				  	  			
  	  			
  				double distance = sm.getDistance(stringA, stringB);
   	  			double weight = distance==0 ? sm.maxDistance() : Math.exp(-distance);
  	  			context.write(new IntWritable(a),new Text(Integer.toString(b) + " " + Double.toString(weight)));
  			}
  		}
  	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "graph creator");
		
		job.setJarByClass(Creator.class);
		job.setMapperClass(MapClass.class);
	  	job.setReducerClass(ReduceClass.class);
	  	job.setMapOutputKeyClass(Text.class);
	  	job.setMapOutputValueClass(Text.class);
	  	job.setOutputKeyClass(IntWritable.class);
	  	job.setOutputValueClass(Text.class);
	  	
	  	job.setNumReduceTasks(50);
	  	job.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", "2500");
	  	
	  	FileInputFormat.addInputPath(job, new Path(args[0]));
	  	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  	job.addCacheFile(new URI("hdfs://localhost:9000/user/hduser/" + args[2]));
	  	
	  	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}