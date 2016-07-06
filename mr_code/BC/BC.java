//By: Joshua Schultz and Jonathan Vieyra
//8/9/2012

/**
Modifications by Chris Joseph:
	-Cleaned up unnecessary code and removed deprecated methods
	-Simplified the generation of normally distributed random numbers
	-Removed the use of a boolean variable ('add') that wasn't doing anything
	-Created some new variables for convenience and simplified math expressions
	-Changed the paths of the input and output folders. Now, each MapReduce
		job will put its output in a folder with the prefix MRi where
		i is the stage (there're 6 stages). The folder also has the name
		of the run (to indicate how many times the program was run) and
		the count (for the 5 update iterations)
	-Added comments
	-Some doubles are so small that when they're appended to Strings, java
		automatically gives them scientific notation. I used the BigDecimal
		class to fix this.
		
Modifications by Corbin McNeill:
	-Adapted algorithm to work both with weighted and unweighted graphs
	-Created input flag parsing code for more dynamic operation.
	-Cleaned code by removing unused variables
*/



import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.util.Iterator;
import java.math.BigDecimal;

public class BC {

	public static void main(String[] args) throws Exception {
		
		//IF ANY OF THESE FLAGS WERE PASSED, PRINT USAGE INSTRUCTIONS AND EXIT
		for (String arg : args) {
			if (arg.equals("-h") || arg.equals("--help") || arg.equals("-help") || arg.equals("-man") || arg.equals("--man")) {
				printUsageInformation();
				System.exit(0);
			}
		}
		
		// BOOLEAN FLAGS, FOR EASILY CONTROLLING WHICH STAGES GET EXECUTED:
		boolean runStage1 = true;
		boolean runStage2 = true;
		boolean runStage3 = true;
		boolean runStage4 = true;
		boolean runStage5 = true;
		boolean runStage6 = true;


		int count = 0;
		long start = new Date().getTime();
		long avg = 0;
		
		int starts = 3; 
		int runs = 1; 
		int iterations = 5;
		String inputPath = "";
		String outputPath = "";

		boolean inputSet = false;
		boolean outputSet = false;
		for (int i=0; i<args.length; i++) {
			String arg = args[i];
			if (arg.length() > 1 && arg.charAt(0) == '-') {
				char flag = arg.charAt(1);
				if (flag == 's' && i+1<args.length) {
					starts = Integer.parseInt(args[++i]);
				} else if (flag == 'r' && i+1<args.length) {
					runs = Integer.parseInt(args[++i]);
				} else if (flag == 'i' && i+1<args.length) {
					iterations = Integer.parseInt(args[++i]);
				} else {
					System.out.println("Invalid flag: " + arg + "\nClosing application.");
					System.exit(1);
				}
			} else if (outputSet) {
				System.out.println("Too many non-optional arguments.\nClosing application.");
				System.exit(1);
			} else if (inputSet) {
				outputPath = arg;
				outputSet = true;
			} else {
				inputPath = arg;
				inputSet = true;
			}
		}
		System.out.println("Starts: " + starts);
		System.out.println("Runs: " + runs);
		System.out.println("Iterations: " + iterations);
		
		if (!outputSet) {
			System.out.println("Input and output paths must be passed in as arguments.\nClosing application.");
			System.exit(1);
		}

		for (int i = 0; i < runs; i++)
		{
			// ----------------  FIRST STAGE  -----------------------
			Configuration conf = new Configuration();
			conf.set("start", Integer.toString(starts));
			
			conf.set("mapreduce.job.reduces", "30");
		  	conf.set("mapreduce.input.fileinputformat.split.maxsize", "250000");

			Job job = Job.getInstance(conf, "Job 1 - Run " + i);
			job.setJarByClass(BC.class);

			job.setMapperClass(Map1.class);
			job.setReducerClass(Reduce1.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			if (i==0) {
				FileInputFormat.addInputPath(job, new Path(inputPath));
			} else {
				FileInputFormat.addInputPath(job, new Path(outputPath + "/MR6-run" + (i-1)));
			}
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/MR1-run" + i));
		
			

			if (runStage1)
				job.waitForCompletion(true);

			// -------------------- SECOND STAGE -------------------------
			Job job2 = Job.getInstance(conf, "Job 2 - Run " + i);
			job2.setJarByClass(BC.class);

			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(outputPath + "/MR1-run" + i));
			FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/MR2-run" + i));

			if (runStage2)
				job2.waitForCompletion(true);

			// THESE ARE THE ITERATIVE STAGES, OR WHAT COHEN CALLS THE 'UPDATE
			// ITERATIONS'.
			
			
			count = 0;
			while(count < iterations) //TODO: make this a for loop
			{
				// --------------  THIRD STAGE ----------------------------

				Job job3 = Job.getInstance(conf, "Job 3 - Run " + i + " - Iter " + count);
				job3.setJarByClass(BC.class);

				job3.setMapperClass(Map3.class);
				job3.setReducerClass(Reduce3.class);

				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				
				if(count == 0) {
					// this is the 1st iteration, so the input will be the output of MR2
					FileInputFormat.addInputPath(job3, new Path(outputPath+ "/MR2-run" + i));
				}
				else
				{
					// the input will be the output of the previous MR4 iteration:
					FileInputFormat.addInputPath(job3, new Path(outputPath + "/MR4-cycle" + (count-1) + "-run" + i));
				}
				FileOutputFormat.setOutputPath(job3, new Path(outputPath + "/MR3-cycle" + count + "-run" + i));

				if (runStage3)
					job3.waitForCompletion(true);


				// ----------------  FOURTH STAGE ------------------------

				Job job4 = Job.getInstance(conf, "Job 4 - Run " + i + " - Iter " + count);
				job4.setJarByClass(BC.class);

				job4.setMapperClass(Map2.class);
				job4.setReducerClass(Reduce2.class);

				job4.setOutputKeyClass(Text.class);
				job4.setOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(job4, new Path(outputPath + "/MR3-cycle" + count + "-run" + i));
				FileOutputFormat.setOutputPath(job4, new Path(outputPath + "/MR4-cycle" + count + "-run" + i));

				if (runStage4)
					job4.waitForCompletion(true);

				count++;
			}

			// -------------------  FIFTH STAGE ----------------------------

			Job job5 = Job.getInstance(conf, "Job 5 - Run " + i);
			job5.setJarByClass(BC.class);

			job5.setMapperClass(Map5.class);
			job5.setReducerClass(Reduce5.class);

			job5.setOutputKeyClass(Text.class);
			job5.setOutputValueClass(Text.class);
			
			// Job 5 will take as its input the output of Job 4.
			// Specifically, it will be the output from the last iteration of Job 4.
			FileInputFormat.addInputPath(job5, new Path(outputPath + "/MR4-cycle" + (count-1) + "-run" + i));
			FileOutputFormat.setOutputPath(job5, new Path(outputPath + "/MR5-run" + i));

			if (runStage5)
				job5.waitForCompletion(true);


			// -------------- SIXTH STAGE -----------------------------

			Job job6 = Job.getInstance(conf, "Job 6 - Run " + i);
			job6.setJarByClass(BC.class);

			job6.setMapperClass(Map6.class);
			job6.setReducerClass(Reduce6.class);

			job6.setOutputKeyClass(Text.class);
			job6.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job6, new Path(outputPath + "/MR5-run" + i));
			if (i < runs-1) {
				FileOutputFormat.setOutputPath(job6, new Path(outputPath + "/MR6-run" + i));
			} else {
				FileOutputFormat.setOutputPath(job6, new Path(outputPath + "/results"));
			}

			if (runStage6)
				job6.waitForCompletion(true);

		}
		long end = new Date().getTime();
		avg = end - start;
		avg = avg / runs;
		System.out.println("Job took an average of " + (avg / 1000.0) + " seconds\n");
	}
	
	public static void printUsageInformation() {
		System.out.print("These are the instructions for this code. Please fill this in.");
		System.out.println();
	}
	
	/** 
	 * Barycentric Clustering Map1
	 */
	static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		/**
		 * Bin each edge by its vertices (two records out for each record in)
		 * 
		 * Input: 
		 * The input value represents a graph edge. 
		 * Input value should contain the following two or three items, in the same order, either space or tab delineated: 
		 * [vertex 1], [vertex 2], and [edge weight] (where edge weight is optional).
		 * 
		 * Output: 
		 * Output key should be one of the edge's vertices. 
		 * Output value should contain the following two or three items, comma delineated: 
		 * [vertex 1], [vertex 2], and [edge weight] (where edge weight is optional), where [vertex 1] is the lesser vertex.
		 */
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			String edge = value.toString();
			String data[];
			data = edge.split(",| |\t");

			if (data[0].compareTo(data[1]) < 0)
			{
				context.write(new Text(data[0]), new Text(StringUtils.join(",", data)));
				context.write(new Text(data[1]), new Text(StringUtils.join(",", data)));
			}
			else if(data[0].compareTo(data[1]) > 0)
			{
				String temp = data[0];
				data[0] = data[1];
				data[1] = temp;
				context.write(new Text(data[0]), new Text(StringUtils.join(",", data)));
				context.write(new Text(data[1]), new Text(StringUtils.join(",", data)));
			}
		}
	}
	
	static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		/**
		 * For each bin (vertex), create a position vector of length r, initialize it with standard
		 * normal samples, and emit a record for
		 * each edge record in the bin, augmenting it with the vertex position vector for that vertex
		 *
		 * Input: 
		 * Input key should be a graph vertex. 
		 * Input value should be a Iterable of edges, each in the following format:
		 * Input value should contain the following two or three items, comma delineated: 
		 * [vertex 1], [vertex 2], and [edge weight] (where edge weight is optional), where [vertex 1] is the lesser vertex.
		 * 
		 * Output: 
		 * Output key should be identical to each separate input value.
		 * Output value should be a combination of the vertex of the the input key and <b>n</b> random gausian-distributed numbers
		 * from the interval [0,1) formated in the following manner: 
		 * "[input key vertex]-[random 1],[random 2],...,[random <b>n</b>]"
		 */
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {


			Configuration conf = context.getConfiguration();
			String start = conf.get("start");
			int starts = Integer.parseInt(start);


			String point = "";
			int i = 0;
			double indepVal = 0.0;
			Random rand = new Random();
			while(i < starts)
			{

				// My version: simpler way to generate random numbers from a Gaussian distribution, in the range [0,1].
				do {
					indepVal = rand.nextGaussian() * 0.5 + 0.5; // normally distributed with both mean and std.dev. = 0.5
				} while (indepVal > 1 || indepVal <= 0);


				String xxx = new BigDecimal(indepVal).toPlainString();
				if (xxx.length() > 10)
					xxx = xxx.substring(0,9);

				if (i+1 == starts)
					point += xxx;
				else
					point += xxx + ",";

				i++;
				// Use BigDecimal to avoid scientific notation from showing up in the string!

			}

			for(Text val : values)
			{
				context.write(new Text(val.toString()), new Text(key.toString() + "-" + point));
			}
		}
	}

	static class Map2 extends Mapper<LongWritable, Text, Text, Text>{

		/**
		 * The identity map.
		 */
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException{
			String[] valueSplit = value.toString().split("\t");
			context.write(new Text(valueSplit[0]), new Text(valueSplit[1]));
		}
	}

	static class Reduce2 extends Reducer<Text,Text,Text,Text>{

		/**
		 * Combine the two records of the same edge with one random vertex position each into one edge containing
		 * both random vertex positions.
		 * 
		 * Input:
		 * Input key represents a graph edge and should contain the following two or three items, comma delineated: 
		 * [vertex 1], [vertex 2], and [edge weight] (where edge weight is optional), where [vertex 1] is the lesser vertex.
		 * Input value should be a iterable of the two records representing random the random vertex positions for the vertices
		 * of that edge. The records should be formatted as: 
		 * "[vertex],[random 1],[random 2],...,[random <b>n</b>]"
		 * 
		 * Output:
		 * Output key should be identical to the input key.
		 * Output value should be the two input values joined with a space.
		 */
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
			StringBuilder points = new StringBuilder();

			Iterator<Text> it = values.iterator();
			String value = it.next().toString();
			points.append(value + " ");

			value = it.next().toString();
			points.append(value); // The second value.


			context.write(new Text(key.toString()),new Text(points.toString()));
		}
	}

	static class Map3 extends Mapper<LongWritable, Text, Text, Text>{
		/**
		 * Create two outputs for every input, keyed by vertex. Repackage edge weight (if applicable).
		 * 
		 * Input:
		 * Input key (passed into this function in the value parameter preceding the a tab character) 
		 * represents a graph edge and should contain the following two or three items, comma delineated: 
		 * [vertex 1], [vertex 2], and [edge weight] (where edge weight is optional), where [vertex 1] is the lesser vertex.
		 * Input value contains two random vertex positions for each of the two vertices of the key edge. They are formatted as:
		 * "[vertex a]-[random a1],[random a2],...,[random a<b>n</b>] [vertex b]-[random b1],[random b2],...,[random b<b>n</b>]"
		 * 
		 * Output: 
		 * Output key should be one of the edge's vertices.
		 * Output value should be a combination of the input value and the edge weight (if graph is weighted) formatted as:
		 * "[input value] [edge weight]"
		 * If the graph is unweighted, output value is identical to input value.
		 */
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {

			String[] valueSplit = value.toString().split("\t");
			String[] data = valueSplit[0].split(",");
			String suffix = data.length>1 ? " " + data[2] : "";
			context.write(new Text(data[0]), new Text(valueSplit[1] + suffix));
			context.write(new Text(data[1]), new Text(valueSplit[1] + suffix));
		}
	}

	static class Reduce3 extends Reducer<Text,Text,Text,Text>{
		/**
		 * Perform an iteration of the Barcentric Clustering to determine new positions for vertices
		 * 
		 * Input: 
		 * Input key should be one of the graph's vertices.
		 * Output value should be an iterable containing all edges adjacent to the input key vertex and the edge weight (if graph is weighted) formatted as:
		 * "[vertex a]-[random a1],[random a2],...,[random a<b>n</b>] [vertex b]-[random b1],[random b2],...,[random b<b>n</b>] [edge weight]"
		 * Omit [edge weight] if not applicable.
		 * 
		 * Output: 
		 * Output key should be identical to each separate input value.
		 * Output value should be a combination of the vertex of the the input key and the <b>n</b> new vertex positions. 
		 * "[input key vertex]-[pos 1],[pos 2],...,[pos <b>n</b>]"
		 */

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String start = conf.get("start"); // number of random starts
			int starts = Integer.parseInt(start);
			String keyVertex = key.toString();

			// copy the values to an ArrayList first:
			ArrayList<String[]> values2 = new ArrayList<String[]>();
			for (Text t:values)
				values2.add(t.toString().split(" "));
				// split at the space char, which separates it to the 2 vertices and respective vectors and the edge weight (if applicable)


			int d = values2.size(); // the degree of this vertex, which is the
			// same as the number of records in this bin.

			

			// For convenience, use two more arrays to represent the vertices
			// and their position vectors. All of these will be indexed by the
			// same value, so we can access them in a straightforward manner.
			String vertices[][] = new String[d][2];
			String positions[][][] = new String[d][2][starts];
			boolean weighted = values2.get(0).length > 2;
			double weights[] = new double[d];
			// Fill in the values:
			for (int i = 0; i < d; i++) {
				String v[] = values2.get(i)[0].split("-");
				// first vertex:
				vertices[i][0] = v[0];
				positions[i][0] = v[1].split(",");

				// second vertex:
				v = values2.get(i)[1].split("-");
				vertices[i][1] = v[0];
				positions[i][1] = v[1].split(",");
				
				if (weighted) {
					weights[i] = Double.parseDouble(values2.get(i)[2]);
				}
			}
			
			double weightSum;
			if (weighted) {
				weightSum = 0;
				for (double weight : weights) {
					weightSum += weight;
				}
			} else {
				weightSum = d;
			}


			// This is a convenient way to keep track of which vertex is NOT the
			// keyVertex in each edge:
			int otherVertexIndex[] = new int[d];
			for (int i = 0; i < d; i++) {
				if (vertices[i][0].equals(keyVertex))
					otherVertexIndex[i] = 1;
				else
					otherVertexIndex[i] = 0;
			}


			String newPositionsVector = "";

			// Outer loop iterates thru the number of random starts
			for (int i = 0; i < starts; i++) {

				double newPositions = 0.0;

				// Inner loop iterates thru the number of edges in this bin.
				for (int j = 0; j < d; j++) {


					// Here we perform one dot product of the matrix multiplication
					// process. One of the values in the position vector
					// of 'keyVertex' will be updated.
					newPositions += (weights[j] / (weightSum + 1)) * Double.parseDouble(positions[j][otherVertexIndex[j]][i]);

				}
				// We also need to add (1/(d + 1))*a to the new position, where
				// a is the current position value of keyVertex
				newPositions += (1.0 / (weightSum + 1)) * Double.parseDouble(positions[0][1 - otherVertexIndex[0]][i]);

				// Add this value to the vector:
				String xxx = new BigDecimal(newPositions).toPlainString();
				if (xxx.length() > 10)
					xxx = xxx.substring(0,9);

				if (i == starts - 1)
					newPositionsVector += xxx;
				else
					newPositionsVector += xxx + ",";
			}

			// At this point, all the new positions have been calculated.
			// We can emit a new record for each edge.
			for (int i = 0; i < d; i++) {
				// Key by edge. The value is the keyVertex plus its new vector.
				context.write(new Text(vertices[i][0] + "," + vertices[i][1] + "," + weights[i]), new Text(keyVertex + "-" + newPositionsVector));
			}
		}
	}


	/*
	 * Map4 and Reduce 4 are identical to Map2 and Reduce2
	static class Map4 extends Mapper<LongWritable, Text, Text ,Text> {

		//identity
		public Map4() {}

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			context.write(new Text(line[0]), new Text(line[1]));
		}
	}

	static class Reduce4 extends Reducer<Text,Text,Text,Text>{

		//combine  the edge records augmented with a single vertex's
		//position vector; output one edge record with all the vertex position vectors
		public Reduce4() {}

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {

			StringBuilder points = new StringBuilder();
			Iterator<Text> it = values.iterator();
			String value = it.next().toString();
			points.append(value + " ");

			value = it.next().toString();
			points.append(value); // The second value.


			context.write(new Text(key.toString()),new Text(points.toString()));

		}
	}
	*/

	static class Map5 extends Mapper<LongWritable, Text, Text ,Text> {
		/**
		 * Get the edge's length averaged over the trials (aij for an edge between i and j)
		 * and emit a record for each vertex containing that average, binned by that vertex
		 * 
		 * Input:
		 * Input key (passed into this function in the value parameter preceding the a tab character) 
		 * represents a graph edge and should contain the following two or three items, comma delineated: 
		 * [vertex 1], [vertex 2], and [edge weight] (where edge weight is optional), where [vertex 1] is the lesser vertex.
		 * Input value contains two random vertex positions for each of the two vertices of the key edge. They are formatted as:
		 * "[vertex a]-[random a1],[random a2],...,[random a<b>n</b>] [vertex b]-[random b1],[random b2],...,[random b<b>n</b>]"
		 * 
		 * Output:
		 * Output key should be one of the vertices from the input edge.
		 * Output value should be the underscore delineated input key and average edge length across different starts.
		 */

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			String[] val = value.toString().split("\t");
			String[] vec = val[1].split(" ");
						
			int idx1 = vec[0].indexOf("-");
			int idx2 = vec[1].indexOf("-");

			String[] vec1 = vec[0].substring(idx1+1).split(","); // The position vector of vertex 1
			String[] vec2 = vec[1].substring(idx2+1).split(","); // The position vector of vertex 2
			
			double avg = 0.0;
			double sum = 0.0;

			// Calculate the avg length of this edge over all trials (the number
			// of random starts). The length of an edge is simply the difference
			// between the positions of its 2 constituent vertices.
			for(int i = 0; i < vec1.length; i++)
			{
				sum += Math.abs(Double.parseDouble(vec1[i]) - Double.parseDouble(vec2[i]));
			}


			avg = sum/vec1.length;
			idx1 = val[0].indexOf(",");
			idx2 = val[0].indexOf(",", idx1+1);
			String ver1 = val[0].substring(0, idx1);
			String ver2 = val[0].substring(idx1+1, idx2);

			String avgString = new BigDecimal(avg).toPlainString();
			if (avgString.length() > 10)
				avgString = avgString.substring(0,9);

			context.write(new Text(ver1), new Text(val[0]+ "_" + avgString));
			context.write(new Text(ver2), new Text(val[0]+ "_" + avgString));
		}
	}

	static class Reduce5 extends Reducer<Text,Text,Text,Text>{
		/**
		 * Sum the lengths of the records in the bin, obtaining aik
		 * 
		 * Input:
		 * Input key should a graph vertex.
		 * Input value should be an iterable containing all the underscore delineated edges and average edge lengths across different starts
		 * adjacent to that vertex. The input value will be of the following form:
		 * "[vertex 1],[vertex 2],[edge weight]_[average length of the edge]"
		 * 
		 * Output:
		 * Output key should be "[vertex 1],[vertex 2],[edge weight]"
		 * Output value should be "[average length of the edge],[degree of input vertex],[sum of edge lengths adjacent to input vertex]"
		 **/

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
			ArrayList<String> valArray = new ArrayList<String>();
			double sum = 0;
			for(Text val : values)
			{
				String temp = val.toString();
				int idx = temp.indexOf("_");
				sum += Double.parseDouble(temp.substring(idx+1));
				valArray.add(temp);
			}
			for(int i=0; i < valArray.size(); i++)
			{
				String temp = valArray.get(i);
				int idx = temp.indexOf("_");

				String stringSum = new BigDecimal(sum).toPlainString();
				if (stringSum.length() > 10)
					stringSum = stringSum.substring(0,9);

				context.write(new Text(temp.substring(0,idx)), new Text(temp.substring(idx+1) + "," + valArray.size() + "," + stringSum));
			}
		}
	}

	static class Map6 extends Mapper<LongWritable,Text,Text,Text> {

		/**
		 * The identity map.
		 */
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException{
			String[] val = value.toString().split("\t");
			context.write(new Text(val[0]), new Text(val[1]));
		}
	}

	static class Reduce6 extends Reducer<Text,Text,Text,Text>{

		/**
		 * Compare the length aij to the neighborhood average Aij (overlined in
		 * the Cohen paper) and emit the edge iff aij <= Aij.
		 * 
		 * Input:
		 * Input key should be an edge formatted as: "[vertex 1],[vertex 2],[edge weight]"
		 * Input value should be "[average length of the edge],[degree of input vertex],[sum of edge lengths adjacent to input vertex]"
		 * te
		 * Output edges remaining after clustering formatted as: "[vertex 1],[vertex 2],[edge weight]"
		 */

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
			int count = 0;
			String[] temp1= null;
			String[] temp2= null;
			for(Text val : values)
			{
				if(count==0)
					temp1 = val.toString().split(",");
				else
					temp2 = val.toString().split(",");
				count++;

			}

			// The following is the calculation of the neighborhood average, as
			// outlined in the Cohen paper. 'aPrime' is the neighborhood average
			double avg = Double.parseDouble(temp1[0]);
			int num1 = Integer.parseInt(temp1[1]);
			double avSum1 = Double.parseDouble(temp1[2]);
			int num2 = Integer.parseInt(temp2[1]);
			double avSum2 = Double.parseDouble(temp2[2]);
			double aPrime = (avSum1 + avSum2 - avg)/(num1 + num2 -1);
			//aPrime+=.001;
			//System.out.println("OUTPUT "+ key.toString()+": "+ avg +"<="+aPrime);
			if(avg <= (aPrime))
			{
				context.write(key, new Text(""));
			}
		}
	}
}
