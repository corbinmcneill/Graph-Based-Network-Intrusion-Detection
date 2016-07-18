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
import java.util.Comparator;
import java.util.PriorityQueue;
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
import SimilarityMeasure.MaximumsNotSetException;
import SimilarityMeasure.SimilarityMeasure;


public class Creator {
	
	private static final String INTERMEDIATE_PATH = "intermediate";
	
	public static class EdgeCreate extends Mapper<LongWritable, Text, Text, Text> {

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
			int number = Integer.parseInt(val.toString().split(" |\t")[0]);
			for (int j = number+1; j<lines.size(); j++) {
				context.write(val, new Text(lines.get(j)));
			}
		}
	}

	public static class DistanceCalc extends Reducer<Text, Text, IntWritable, Text> {
		
		SimilarityMeasure sm = new EuclideanSquared();
		
		@Override
		public void setup(Context context) throws IOException ,InterruptedException {
			String maximumsString = context.getConfiguration().get("maximums");
			if (maximumsString != null) {
				double a[] = new double[maximumsString.split(",").length];
				String maxSplit[] = maximumsString.substring(1, a.length-1).split(",");
				for (int i=0; i<a.length; i++) {
					a[i] = Double.parseDouble(maxSplit[i]);	
				}
				sm.setMaximums(a);
			}
		};

  		@Override
  		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
  			
  			StringTokenizer splitA1 = new StringTokenizer(key.toString());
  			int a = Integer.parseInt(splitA1.nextToken());
  			String[] stringA = splitA1.nextToken().split(" |,");
  			
  			for (Text val : vals) {
  				
  				StringTokenizer splitB1 = new StringTokenizer(val.toString());
  				int b = Integer.parseInt(splitB1.nextToken());
  				String[] stringB = splitB1.nextToken().split(" |,");  				  	  			
  	  			
  				try {
  					double distance = sm.getDistance(stringA, stringB);
  					double weight = distance==0 ? sm.maxDistance() : Math.exp(-distance);
  					
  					context.write(new IntWritable(a),new Text(Integer.toString(b) + " " + Double.toString(weight)));
  				} catch (MaximumsNotSetException e) {
  					e.printStackTrace();
  				}
   	  			
  	  		}
  		}
  	}
	
	public static class BinByVertex extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			String values[] = val.toString().split(" |\t");
			context.write(new Text(values[0]), new Text(values[1] + " " + values[2]));
			context.write(new Text(values[1]), new Text(values[0] + " " + values[2]));
		}
	}
	
	public static class kNNFilter extends Reducer<Text, Text, Text, Text> {
		
		public static class Datum {
			public int v;
			public double w;
			
			public Datum(int vertex, double weight) {
				this.v = vertex;
				this.w = weight;
			}
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			int kVal = Integer.parseInt(context.getConfiguration().get("kVal"));
			PriorityQueue<Creator.kNNFilter.Datum> pq = new PriorityQueue<Creator.kNNFilter.Datum>(kVal + 1, new Comparator<Creator.kNNFilter.Datum>() {
				@Override
				public int compare(Datum o1, Datum o2) {
					if (o1.w > o2.w) {
						return 1;
					} else if (o1.w < o2.w) {
						return -1;
					} else {
						return 0;
					}
				}
			});
			
			//build the priority queue
			for (Text val : vals) {
				String stringVal[] = val.toString().split(" |\t");
				pq.offer(new Datum(Integer.parseInt(stringVal[0]), Double.parseDouble(stringVal[1])));
				if (pq.size() > kVal) {
					pq.remove();
				}
			}
			
			for (Datum d : pq) {
				if (Integer.parseInt(key.toString()) < d.v) {
					context.write(new Text(key.toString() + " " + Integer.toString(d.v)), new Text(Double.toString(d.w)));
				} else {
					context.write(new Text(Integer.toString(d.v) + " " + key.toString()), new Text(Double.toString(d.w)));
				}
			}
		}
	}
	
	public static class IdentityMap extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			String values[] = val.toString().split("\t");
			context.write(new Text(values[0]), new Text(values[1]));
		}
	}
	
	public static class RemoveDuplicateEdges extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			context.write(key, vals.iterator().next()); //return the first value since all the values are identitcal
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "25000");
		conf.set("mapreduce.job.reduces", "50");
		conf.set("kVal", args[3]);
		
		/* GRAPH CREATION */
		Job job2 = Job.getInstance(conf, "edge creation");
		
		job2.setJarByClass(Creator.class);
		job2.setMapperClass(EdgeCreate.class);
	  	job2.setReducerClass(DistanceCalc.class);
	  	job2.setMapOutputKeyClass(Text.class);
	  	job2.setMapOutputValueClass(Text.class);
	  	job2.setOutputKeyClass(IntWritable.class);
	  	job2.setOutputValueClass(Text.class);
	  		  	
	  	FileInputFormat.addInputPath(job2, new Path(args[0]));
	  	FileOutputFormat.setOutputPath(job2, new Path(INTERMEDIATE_PATH+"1"));
	  	job2.addCacheFile(new URI("hdfs://localhost:9000/user/hduser/" + args[2]));
	  	
	  	job2.waitForCompletion(true); 	
	  		  	
	  	/* KNNG TRIMMING */
	  	
	  	Job job3 = Job.getInstance(conf, "trimming 1");
	  	
		job3.setJarByClass(Creator.class);
		job3.setMapperClass(BinByVertex.class);
	  	job3.setReducerClass(kNNFilter.class);
	  	job3.setMapOutputKeyClass(Text.class);
	  	job3.setMapOutputValueClass(Text.class);
	  	job3.setOutputKeyClass(Text.class);
	  	job3.setOutputValueClass(Text.class);

	  	FileInputFormat.addInputPath(job3, new Path(INTERMEDIATE_PATH+"1"));
	  	FileOutputFormat.setOutputPath(job3, new Path(INTERMEDIATE_PATH+"2"));

	  	job3.waitForCompletion(true);
	  	
	  	Job job4 = Job.getInstance(conf, "trimming 2");
	  	job4.setJarByClass(Creator.class);
		job4.setMapperClass(IdentityMap.class);
	  	job4.setReducerClass(RemoveDuplicateEdges.class);
	  	job4.setMapOutputKeyClass(Text.class);
	  	job4.setMapOutputValueClass(Text.class);
	  	job4.setOutputKeyClass(Text.class);
	  	job4.setOutputValueClass(Text.class);
	  	
	  	FileInputFormat.addInputPath(job4, new Path(INTERMEDIATE_PATH+"2"));
	  	FileOutputFormat.setOutputPath(job4, new Path(args[1]));

	  	job4.waitForCompletion(true);
	}
}
