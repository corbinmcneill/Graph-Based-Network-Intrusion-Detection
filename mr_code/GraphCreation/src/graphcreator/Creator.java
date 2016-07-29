package graphcreator;
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
import SimilarityMeasure.MaximumsNotSetException;
import SimilarityMeasure.SimilarityMeasure;

/* USAGE WARNING:
 * 
 * "java.lang.OutOfMemoryError: Java heap space" can frequently occur in this code 
 * in (at least) two circumstances:
 * 
 * 1) Not enough mappers. This causes sort and shuffle algorithms to exceed available JVM space
 * 2) Too many mappers. This causes internal fragmentation among hdfs blocks.
 * 
 * The below Hadoop configurations have scaled well up to 25,000 records. Behavior when records
 * exceed 25,000 is unknown.
 */

/**
 * This MapReduce Creator class can create both fully complete graphs and k-Nearest Neighbor
 * Graphs (kNNGs).
 * @author mcneill
 */
public class Creator {
	/**
	 * The base path to use for intermediate hdfs storage
	 */	
	private static final String INTERMEDIATE_PATH = "/intermediate";
	
	public static class EdgeCreate extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * An ArrayList that holds all the lines of the distributed cache.
		 */
		ArrayList<String> lines = new ArrayList<String>();
		
		/**
		 * Populate lines with the distributed cache.
		 */
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
	  
		/**
		 * Use the input and the distributed cache to pair vertices.
		 */
		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			int number = Integer.parseInt(val.toString().split(" |\t")[0]);
			// the initial value of j prohibits duplicates
			for (int j = number+1; j<lines.size(); j++) {
				context.write(val, new Text(lines.get(j)));
			}
		}
	}

	/**
	 * Calculate edge weights for each vertex pair
	 * @author mcneill
	 */
	public static class DistanceCalc extends Reducer<Text, Text, IntWritable, Text> {
		
		/**
		 * The similarity measure to be used for distance calculations.
		 */
		SimilarityMeasure sm = new EuclideanSquared();
		
		/**
		 * This setup method reads the maximum values property and passes the maximums to the 
		 * similarity measure. This code was usually not used (if the maximums property is not 
		 * set this code has no effect) and maximums were rather implemented with literals in
		 * relevant similarity measures.
		 */
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

		/**
		 * This mapper inputs vertex pairs (binned by the lesser vertex) with their associated data vectors and
		 * outputs the vertex pair with the associated edge weight.
		 */
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
	
	/**
	 * This mapper bins edges by both vertices
	 * @author mcneill
	 */
	public static class BinByVertex extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			String values[] = val.toString().split(" |\t");
			context.write(new Text(values[0]), new Text(values[1] + " " + values[2]));
			context.write(new Text(values[1]), new Text(values[0] + " " + values[2]));
		}
	}
	
	/**
	 * This reducer takes edges binned by vertex and emits only the k highest weighted edges, where k is the 
	 * value of the property "kVal"
	 * @author mcneill
	 */
	public static class kNNFilter extends Reducer<Text, Text, Text, Text> {
				
		@Override
		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			int kVal = Integer.parseInt(context.getConfiguration().get("kVal"));
			int kEdgesV[] = new int[kVal];
			double kEdgesW[] = new double[kVal];
			int minPos = 0;
			double minWeight = 0;
			
			for (Text val : vals) {
				String split[] = val.toString().split(" ");
				int v = Integer.parseInt(split[0]);
				double w = Double.parseDouble(split[1]);
				
				if (w > minWeight) {
					kEdgesV[minPos] = v;
					kEdgesW[minPos] = w;
					
					minWeight = Double.MAX_VALUE;
					for (int i=0; i<kVal; i++) {
						if (kEdgesW[i] < minWeight) {
							minPos = i;
							minWeight = kEdgesW[i];
						}
					}
				}
			}

			for (int i=0; i<kVal; i++) {
				if (Integer.parseInt(key.toString()) < kEdgesV[i]) {
					context.write(new Text(key.toString() + " " + Integer.toString(kEdgesV[i])), new Text(Double.toString(kEdgesW[i])));
				} else {
					context.write(new Text(Integer.toString(kEdgesV[i]) + " " + key.toString()), new Text(Double.toString(kEdgesW[i])));
				}
			}
		}
	}
	
	/**
	 * This is the identity map. It passes input as output, unaffected.
	 * @author mcneill
	 */
	public static class IdentityMap extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			String values[] = val.toString().split("\t");
			context.write(new Text(values[0]), new Text(values[1]));
		}
	}
	
	/**
	 * This reducer takes weighted edges, binned by the sorted vertex values of the edge and removes duplicates.
	 * @author mcneill
	 */
	public static class RemoveDuplicateEdges extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			context.write(key, vals.iterator().next()); //return the first value since all the values are identitcal
		}
	}


	public static void main(String[] args) throws Exception {
		
		/*
		 * args[0] is the hdfs input path
		 * args[1] is the hdfs output path
		 * args[2] is the location of the single input file
		 * args[3] is the k value for kNNG creation, -1 if a full graph is to be used
		 */
		
		/**
		 * conf1 gives a smaller amount of input to each MapReduce job, making it optimal for MR jobs where output
		 * or intermediate data volume is significantly larger than input data volume
		 */
		Configuration conf1 = new Configuration();
		conf1.set("mapreduce.input.fileinputformat.split.maxsize", "5000");
		conf1.set("mapreduce.job.split.metainfo.maxsize", "-1");
		conf1.set("mapreduce.job.reduces", "100");
		
		/**
		 * conf2 is optimal for MapReduce jobs where data volume is roughly consistent throughout the MR job.
		 */
		Configuration conf2 = new Configuration();
		conf2.set("mapreduce.input.fileinputformat.split.maxsize", "5000000");
		conf2.set("mapreduce.job.split.metainfo.maxsize", "-1");
		conf2.set("mapreduce.job.reduces", "100");
		conf2.set("kVal", args[3]);
		
		/* GRAPH CREATION */
		Job job1 = Job.getInstance(conf1, "edge creation");
		
		job1.setJarByClass(Creator.class);
		job1.setMapperClass(EdgeCreate.class);
	  	job1.setReducerClass(DistanceCalc.class);
	  	job1.setMapOutputKeyClass(Text.class);
	  	job1.setMapOutputValueClass(Text.class);
	  	job1.setOutputKeyClass(IntWritable.class);
	  	job1.setOutputValueClass(Text.class);
	  		  	
	  	FileInputFormat.addInputPath(job1, new Path(args[0]));
  		job1.addCacheFile(new URI("hdfs://localhost:9000/user/hduser/" + args[2]));

	  	if (!args[3].equals("-1")) {
		  	FileOutputFormat.setOutputPath(job1, new Path(args[1] + INTERMEDIATE_PATH+"1"));
	  	} else {
		  	FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/output"));
	  	}
	  	
	  	job1.waitForCompletion(true);
	  	
	  	/*
	  	 * Only run KNNG trimming MapReduce jobs if kNNG is desired, as oppused to a complete graph.
	  	 */
	  	if (!args[3].equals("-1")) {
	  	
	  		Job job2 = Job.getInstance(conf2, "trimming 1");
	  	
	  		job2.setJarByClass(Creator.class);
	  		job2.setMapperClass(BinByVertex.class);
	  		job2.setReducerClass(kNNFilter.class);
	  		job2.setMapOutputKeyClass(Text.class);
	  		job2.setMapOutputValueClass(Text.class);
	  		job2.setOutputKeyClass(Text.class);
	  		job2.setOutputValueClass(Text.class);

	  		FileInputFormat.addInputPath(job2, new Path(args[1] + INTERMEDIATE_PATH+"1"));
	  		FileOutputFormat.setOutputPath(job2, new Path(args[1]+INTERMEDIATE_PATH+"2"));

	  		job2.waitForCompletion(true);
	  	
	 		Job job3 = Job.getInstance(conf2, "trimming 2");
	 		job3.setJarByClass(Creator.class);
			job3.setMapperClass(IdentityMap.class);
			job3.setReducerClass(RemoveDuplicateEdges.class);
		  	job3.setMapOutputKeyClass(Text.class);
		  	job3.setMapOutputValueClass(Text.class);
		  	job3.setOutputKeyClass(Text.class);
		  	job3.setOutputValueClass(Text.class);
		  	
		  	FileInputFormat.addInputPath(job3, new Path(args[1] + INTERMEDIATE_PATH+"2"));
		  	FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/output"));
	
		  	job3.waitForCompletion(true);
	  	}
	}
}
