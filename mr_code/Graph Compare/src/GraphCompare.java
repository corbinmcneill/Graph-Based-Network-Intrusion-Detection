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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GraphCompare {

	public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			context.write(val, new IntWritable(1));
		} 
	}

	public static class Reduce1 extends Reducer<Text, IntWritable, IntWritable, IntWritable> {
		
  		@Override 
  		public void reduce(Text key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
  			int size = 0;
  			for (@SuppressWarnings("unused") IntWritable i : vals) {
  				size++;
  			}
  			context.write(new IntWritable(size), new IntWritable(1));
  		}
  	}
	
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			String[] keyval = val.toString().split("\t");
			context.write(new IntWritable(Integer.parseInt(keyval[0])), new IntWritable(Integer.parseInt(keyval[1])));
		}
	}
	
	public static class Reduce2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
  		public void reduce(IntWritable key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : vals) {
				sum += Integer.parseInt(val.toString());
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "graph compare 1");
		
		job1.setJarByClass(GraphCompare.class);
		job1.setMapperClass(Map1.class);
	  	job1.setReducerClass(Reduce1.class);
	  	job1.setMapOutputKeyClass(Text.class);
	  	job1.setMapOutputValueClass(IntWritable.class);
	  	job1.setOutputKeyClass(IntWritable.class);
	  	job1.setOutputValueClass(IntWritable.class);
	  	
	  	job1.setNumReduceTasks(30);
	  	job1.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", "25000");
	  	
	  	FileInputFormat.addInputPath(job1, new Path(args[0]));
	  	FileInputFormat.addInputPath(job1, new Path(args[1]));
	  	FileOutputFormat.setOutputPath(job1, new Path(args[2]));
	  	
	  	job1.waitForCompletion(true);
	  	
	  	Job job2 = Job.getInstance(conf, "graph compare 2");
	  	job2.setJarByClass(GraphCompare.class);
	  	job2.setMapperClass(Map2.class);
	  	job2.setReducerClass(Reduce2.class);
	  	job2.setMapOutputKeyClass(IntWritable.class);
	  	job2.setMapOutputValueClass(IntWritable.class);
	  	job2.setOutputKeyClass(IntWritable.class);
	  	job2.setOutputValueClass(IntWritable.class);
	  	
	  	job2.setNumReduceTasks(30);
	  	
	  	FileInputFormat.addInputPath(job2, new Path(args[2]));
	  	FileOutputFormat.setOutputPath(job2, new Path(args[3]));
	  	
	  	job2.waitForCompletion(true);
	}
}
