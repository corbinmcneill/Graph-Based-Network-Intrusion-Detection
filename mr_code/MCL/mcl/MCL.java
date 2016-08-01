package mcl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

/**
This is our MCL Algorithm in Hadoop. Each MapReduce job below has comments
associated with them, which explain their functionality.

The matrix multiplication is based on the algorithm by Norstad
(http://www.norstad.org/matrix-multiply/). Our algorithm has slight modifications
in order to be better optimized for the MCL algorithm, rather than just
performing multiplication. We have implemented the algorithm based on Norstad's
Strategy 3.

The rest of the algorithm is based on the original MCL algorithm by Stijn van Dongen:
(http://micans.org/mcl/index.html).

@author Chris Joseph and Stephen Krucelyak
*/

public class MCL extends Configured implements Tool {

	// ------------  THESE ARE SOME IMPORTANT GLOBAL VARIABLES! -------------
    static int t = 3000;                // SUBMATRIX SIZE
    static int R;                       // NUMBER OF REDUCERS

    public static void main(String args[]) throws Exception {
        // For timekeeping/statistics:
        long start = new Date().getTime();
        long avg = 0;

        int runs = ToolRunner.run(new Configuration(), new MCL(), args);

        long end = new Date().getTime();
        avg = end - start;
        avg = avg / runs;
        System.out.println("Job took an average of " + (avg / 1000.0) + " seconds\n");

        System.exit(0);
    }

    @Override
    public int run(String args[]) throws Exception {
        // BOOLEAN FLAGS, FOR EASILY CONTROLLING WHICH STAGES GET EXECUTED:
        boolean runStage1 = true;
        boolean runStage2 = true;
        boolean runStage3 = true;
        boolean runStage4 = true;
        boolean runStage5 = true;
        boolean runStage6 = true;


        // Parse the command line variables:
        // args[0] = input location, 
        // args[1] = output location
        // args[2] = number of runs
        // args[3] = number of vertices
        // args[4] = number of reducers (I set this to the number of my processor's cores)
        // args[5] = number of cycles (for expansion & inflation)
        // args[6] = inflation parameter (the power that each col. is raised to)
        int runs = Integer.parseInt(args[2]);
        long n = Long.parseLong(args[3]);
        R = Integer.parseInt(args[4]);
        int CYCLES = Integer.parseInt(args[5]);
        double INFLATION_PARAMETER = Double.parseDouble(args[6]);

        long p = (n-1)/t+1; // number of submatrices per row/col (?)

        Configuration conf = this.getConf();
        // Store some of the command line variables in the conf object:
        conf.set("t", t + "");
        conf.set("p", p + "");
        conf.set("INFLATION_PARAMETER", INFLATION_PARAMETER + "");


        // The program starts running here.
        for (int i = 0; i < runs; i++) {

            // ---------------------- FIRST STAGE ---------------------------
            Job job1 = Job.getInstance(conf, "Job 1 - Run " + i);
            job1.setJarByClass(MCL.class);

            job1.setMapperClass(Map1.class);
            job1.setReducerClass(Reduce1.class);
            job1.setNumReduceTasks(R);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1] + "-MR1-run" + i));

            if (runStage1)
                job1.waitForCompletion(true);



            int counter = 0;
            while (counter < CYCLES) {

                // ---------------------- SECOND STAGE -----------------------
                Job job2 = Job.getInstance(conf, "Job 2 - Cycle " + counter + " - Run " + i);
                job2.setJarByClass(MCL.class);

                job2.setMapperClass(Map2.class);
                job2.setPartitionerClass(Partition2.class);
                job2.setReducerClass(Reduce2.class);
		        job2.setNumReduceTasks(R);

                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);

                if (counter == 0)
                    // MR2's input will be the output of MR1.
                    FileInputFormat.addInputPath(job2, new Path(args[1] + "-MR1-run" + i));
                else
                    // MR2's input will be the output of MR3 from the previous iteration.
                    FileInputFormat.addInputPath(job2, new Path(args[1] + "-MR3-run" + i + "-cycle" + (counter - 1)));

                FileOutputFormat.setOutputPath(job2, new Path(args[1] + "-MR2-run" + i + "-cycle" + counter));

                // Set the external JBLAS jar:
                Path pfad = new Path("ClassFiles/jblas.jar");
                job2.addCacheFile(pfad.toUri());

                if (runStage2)
                    job2.waitForCompletion(true);


                // ---------------------- THIRD STAGE ---------------------------
                Job job3 = Job.getInstance(conf, "Job 3 - Cycle " + counter + " - Run " + i);
                job3.setJarByClass(MCL.class);

                job3.setMapperClass(Map3.class);
                job3.setReducerClass(Reduce3.class);
                job3.setNumReduceTasks(R);

                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job3, new Path(args[1] + "-MR2-run" + i + "-cycle" + counter));
                FileOutputFormat.setOutputPath(job3, new Path(args[1] + "-MR3-run" + i + "-cycle" + counter));

                if (runStage3)
                    job3.waitForCompletion(true);

                counter++;

            } // end while-loop

            // -------------------- FOURTH STAGE -------------------------------
            Job job4 = Job.getInstance(conf, "Job 4 - Run " + i);
            job4.setJarByClass(MCL.class);

            job4.setMapperClass(Map4.class);
            job4.setReducerClass(Reduce4.class);
            job4.setNumReduceTasks(R);

            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job4, new Path(args[1] + "-MR3-run" + i + "-cycle" + (counter-1)));
            FileOutputFormat.setOutputPath(job4, new Path(args[1] + "-MR4-run" + i));

            if (runStage4)
                job4.waitForCompletion(true);


            // -------------------- FIFTH STAGE -------------------------------
            Job job5 = Job.getInstance(conf, "Job 5 - Run " + i);
            job5.setJarByClass(MCL.class);

            job5.setMapperClass(Map5.class);
            job5.setReducerClass(Reduce5.class);
            job5.setNumReduceTasks(R);

            job5.setOutputKeyClass(Text.class);
            job5.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job5, new Path(args[1] + "-MR4-run" + i));
            FileOutputFormat.setOutputPath(job5, new Path(args[1] + "-MR5-run" + i));

            if (runStage5)
                job5.waitForCompletion(true);


            // -------------------- SIXTH STAGE -------------------------------
            Job job6 = Job.getInstance(conf, "Job 6 - Run " + i);
            job6.setJarByClass(MCL.class);

            job6.setMapperClass(Map6.class);
            job6.setReducerClass(Reduce6.class);
            job6.setNumReduceTasks(R);

            job6.setOutputKeyClass(Text.class);
            job6.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job6, new Path(args[1] + "-MR5-run" + i));
            FileInputFormat.addInputPath(job6, new Path(args[0]));
            FileOutputFormat.setOutputPath(job6, new Path(args[1] + "-MR6-run" + i));

            if (runStage6)
                job6.waitForCompletion(true);

        } // end for-loop

        return runs;
    }



    // ---------------- THE MAPREDUCE STAGES BEGIN HERE ----------------------

	/** Map 1.
	The purpose of the first MapReduce job is to process the raw edge file and
	create columns of the matrix, since that's what the subsequent jobs require
    as input. In the paper by Gong, his algorithm's first step assumes we have
    this step completed already. The columns are also normalized in this job's
    reducer (thereby producing the Markov Matrix).
	*/
	static class Map1 extends Mapper<LongWritable, Text, Text, Text>{

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {

			String nodes[];
			nodes = value.toString().split(" |\t");

			// To send each edge to the appropriate reducer, emit
			// a record with one of the vertices as the key and the other as the
			// value.
			context.write(new Text(nodes[0]), new Text(nodes[1] + " " + (nodes.length>2? nodes[2]:"1")));
			context.write(new Text(nodes[1]), new Text(nodes[0] + " " + (nodes.length>2? nodes[2]:"1")));

			// Since the graph is undirected, emit 2 records.

		}
	}

	/** Reduce 1.
	This reducer creates a single column of the matrix. The key k will be index
    of the column, and each of the values Vi represent an edge between nodes k
    and Vi.
	*/
	static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {

            // Use an arraylist to store all the values. This provides us an
            // easy way to count the total sum for this column, which will be
            // needed to normalize all the values.
            
			/*String k = key.toString();
            ArrayList<String> vals = new ArrayList<String>();
            for (Text t : values) {
                String s = t.toString();
                if (!s.equals(k)) {
                    // We will manually add the self-edge later.
                    vals.add(t.toString());
                }
            }*/
			ArrayList<String> adjacentV = new ArrayList<String>();
			ArrayList<Double> adjacentW = new ArrayList<Double>();
			double weightSum = 0;
			for (Text t : values) {
				String s[] = t.toString().split(" ");
				double d = Double.parseDouble(s[1]);
				adjacentV.add(s[0]);
				adjacentW.add(d);
				weightSum += d;
			}
			
			weightSum += weightSum/adjacentV.size();

			// Now that we have the array representing this col, we can output that
			// as a single file:
            // String normalizedVal =  String.format("%.10f", (1.0 / (vals.size() + 1)));
            String column = key.toString() + ":" + weightSum/(adjacentV.size() + 1) + ",";     // The self-edge
            // Using 10 digits of precision in the doubles.

            // We don't deal with weighted edges, so each entry in the matrix
            // will have an initial value of 1, which makes the normalization
            // easy.

			// The format is: a:X,b:X,c:X,....
			// where a,b,c,... are the indices of the row; 'X' is the entry in
            // the matrix in that cell.

            for (int i = 0; i < adjacentV.size(); i++) {
                column += adjacentV.get(i) + ":" + adjacentW.get(i)/weightSum + ",";
            }

			// Remove the extra comma at the end:
			column = column.substring(0, column.length() - 1);

			context.write(key, new Text(column));
		}
	}



	// --------------------------------------------------------------------- //
	// THE FOLLOWING 2 STAGES ARE THE MATRIX MULTIPLICATION MAPREDUCE STAGES.
	// --------------------------------------------------------------------- //

	/** Map 2.
    This is the first Map of the block matrix multiplication. This map is almost
	identical to the one described by Norstad, except that the inputs are
    column vectors, not individual edges.
    */
    static class Map2 extends Mapper<LongWritable, Text, Text, Text>{

        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int t = Integer.parseInt(conf.get("t")); // submatrix size
            long p = Long.parseLong(conf.get("p")); // number of blocks/submatrices

            String str1[] = value.toString().split("\t");
            long col = Long.parseLong(str1[0]);
            String str2[] = str1[1].split(",");
            for (String str : str2) {
                String str3[] = str.split(":");
                long row = Long.parseLong(str3[0]);
                String entry = str3[1];

                // The following block of code follows the same format as
                // Norstad's map function. Since we are multiplying the same
                // matrix by itself, we don't have a matrix A and B; instead,
                // each edge is considered twice, once as if it's from matrix A
                // and again as if it's from matrix B.

                // First, treat this edge as if it's from matrix A (the left matrix):
                // Edge from matrix A with key=(i,k) and value=a(i,k):
                //     for 0 <= jb < NJB:
                //         emit (k/KB, jb, i/IB), (i mod IB, k mod KB, a(k,j))
                for (long jb = 0; jb < p; jb++) {
                    String keyOutput = (col/t) + ":" + jb + ":" + (row/t);
                    String valOutput = (row%t) + ":" + (col%t) + ":" + entry;
                    context.write(new Text(keyOutput), new Text(valOutput));
                }

                // Next, treat this edge as if it's from matrix B (the right matrix):
                // Edge from matrix B with key=(k,j) and value=b(k,j):
                //     emit (k/KB, j/JB, -1), (k mod KB, j mod KB, b(k,j))
                String keyOutput = (row/t) + ":" + (col/t) + ":-1";
                String valOutput = (row%t) + ":" + (col%t) + ":" + entry;
                context.write(new Text(keyOutput), new Text(valOutput));
            }
        }
    }

    /** Partition 2.
    Our own custom partitioner for the first MapReduce job. Follows directly
    from Norstad's formula for the partitioner.
    */
    static class Partition2 extends Partitioner<Text, Text> implements Configurable {
        private Configuration conf;

    	public int getPartition(Text key, Text value, int numPartitions) {

            // The partitioner maps intermediate key (kb, jb, ib) to a reducer r as follows:
            // r = (jb*KB + kb) mod R
            String str1[] = value.toString().split(":");
            long kb = Long.parseLong(str1[0]);
            long jb = Long.parseLong(str1[1]);
            int t = Integer.parseInt(conf.get("t"));
            int R = Integer.parseInt(conf.get("R"));

            return (int)((jb*t + kb) % R);

    	}

        public Configuration getConf() {
            return conf;
        }
        public void setConf(Configuration conf) {
            this.conf = conf;
        }
    }


    /** Reduce 2.
    This is the first Reduce of the block matrix multiplication.
    */
    static class Reduce2 extends Reducer<Text, Text, Text, Text> {

        // These may need to be static!!!
        double A[][] = new double[t][t];
        double B[][] = new double[t][t];
        //DoubleMatrix A = new DoubleMatrix(t, t);
        //DoubleMatrix B = new DoubleMatrix(t, t);

        long skb = -1;
        long sjb = -1;

        protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int t = Integer.parseInt(conf.get("t")); // submatrix size

            // If key is of the form (kb, jb, ib) with ib = -1:
            String keystr[] = key.toString().split(":");
            long kb = Long.parseLong(keystr[0]);
            long jb = Long.parseLong(keystr[1]);
            long ib = Long.parseLong(keystr[2]);

            if (ib == -1) {
                skb = kb;
                sjb = jb;
                // B.fill(0.0);
                // for each value = (k, j, v) in valueList: do B(k,j) = v
                for (Text val : values) {
                    String valstr[] = val.toString().split(":");
                    int k = Integer.parseInt(valstr[0]);
                    int j = Integer.parseInt(valstr[1]);
                    double v = Double.parseDouble(valstr[2]);

                    B[k][j] = v;
                    // B.put(k, j, v);
                }
            }

            else {
                if (kb != skb || jb != sjb)
                    return;
                // A.fill(0.0);
                // for each value = (i, k, v) in valueList: do A(i,k) = v
                for (Text val : values) {
                    String valstr[] = val.toString().split(":");
                    int i = Integer.parseInt(valstr[0]);
                    int k = Integer.parseInt(valstr[1]);
                    double v = Double.parseDouble(valstr[2]);

                    A[i][k] = v;
                    // A.put(i, k, v);
                }

                // Now multiply the blocks and emit the results:
                long ibase = ib * t;
                long jbase = jb * t;

                // THIS BLOCK OF CODE WAS FOR THE OLD WAY OF DOING MULTIPLICATION
                
                for (int i = 0; i < A.length; i++) {
                    for (int j = 0; j < A.length; j++) {
                        double sum = 0.0;
                        for (int k = 0; k < A.length; k++) {
                            sum += A[i][k] * B[k][j];
                        }
                        // if sum != 0 emit (ibase+i, jbase+j), sum
                        if (Math.abs(sum) > 0.00001) {
                            String sumString = String.format("%.10f", sum);
                            context.write(new Text((ibase + i) + ":" + (jbase + j)), new Text(sumString));
                        }
                    }
                }
                

                // NEW WAY OF DOING MULTIPLICATION USING JBLAS: CREATE A
                // TEMPORARY MATRIX TO HOLD THE RESULT OF A*B AND THEN CALL
                // A LIBRARY FUNCTION TO DO THE MULTIPLICATION.
                /*DoubleMatrix tempMatrix = A.mmul(B);
                for (int i = 0; i < t; i++) {
                    for (int j = 0; j < t; j++) {
                        double d = tempMatrix.get(i, j);
                        if (Math.abs(d) > 0.00001) {
                            String sumString = String.format("%.10f", d);
                            context.write(new Text((ibase + i) + ":" + (jbase + j)), new Text(sumString));
                        }
                    }
                }*/
            }
        }
    }


    /** Map 3.
    This is the mapper for the 2nd stage of the matrix multiplication. This is
    not the same as what Norstad specifies in his algorithm, but slightly
    different. Instead of an identity map, this mapper keys the input by
    COLUMN, so that in the subsequent reducer, the column vectors can be
    reconstructed again.
    */
    static class Map3 extends Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String str1[] = value.toString().split("\t");
            String entry = str1[1];
            String str2[] = str1[0].split(":");
            String row = str2[0];
            String col = str2[1];

            context.write(new Text(col), new Text(row + ":" + entry));
        }
    }

    /** Reduce 3.
    This is the 2nd (and last) reduce function for the block matrix multiplication.
    The key is the global column index; the values are of the form row:entry.
    The same row may be repeated multple times, so their entries need to be
    summed up.

    Because each reducer receives data for an entire column, it's possible to
    do the inflation in the same step, thus saving an extra MapReduce stage.
    The output is a column vector that is raised to the inflation parameter
    and then normalized.
    */
    static class Reduce3 extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            double INFLATION_PARAMETER = Double.parseDouble(conf.get("INFLATION_PARAMETER"));

            HashMap<String, Double> hashtable = new HashMap<String, Double>();

            String str[];
            for (Text val : values) {
                str = val.toString().split(":");
                String row = str[0];
                double entry = Double.parseDouble(str[1]);

                if (hashtable.containsKey(row)) {
                    // Update the value in the hashtable
                    double d = hashtable.get(row);
                    hashtable.put(row, d + entry);
                }
                else {
                    // Insert the value for the first time
                    hashtable.put(row, entry);
                }
            }


            // After we collect all the values, it's time to perform the inflation.
            // Raise every value to the power specified (INFLATION_PARAMETER)
            // and then normalize the column.
            double sum = 0.0; // The total sum for this column
            Set<String> keys = hashtable.keySet();
            for (String row : keys) {
                double d = Math.pow(hashtable.get(row), INFLATION_PARAMETER);
                sum += d;
                hashtable.put(row, d);
            }


            // Next, create a single string representing everything in this column.
            // We divide each entry by the sum to complete the normalization.
            keys = hashtable.keySet();
            String keyOutput = "";
            for (String row : keys) {
                keyOutput += row + ":" + String.format("%.10f", (hashtable.get(row) / sum)) + ",";
            }
            // Remove the extra comma at the end:
            keyOutput = keyOutput.substring(0, keyOutput.length() - 1);

            context.write(new Text(key.toString()), new Text(keyOutput));
        }
    }

    /**
    Map 4.
    After the expansion/inflation iterations are completed, this MR will parse
    the final matrix to determine the clusters.
    This mapper will split up its input (which is a col vector) and key the
    data by row index, so that the reducers can reconstruct row vectors.
    */
    static class Map4 extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String str1[] = value.toString().split("\t");
            String col = str1[0];
            String rowsAndValues[] = str1[1].split(",");

            String row, val;
            String tmp[];
            for (String rowAndValue : rowsAndValues) {
                // Send matrix entries belonging to the same row to the same reducer:
                tmp = rowAndValue.split(":");
                row = tmp[0];
                val = tmp[1];
                context.write(new Text(row), new Text(col + ":" + val));
            }
        }
    }

    /**
    Reuce 4.
    This reducer receives a row and a list of its values. It parses the row to
    determine which columns have the same values, so that the clusters can be
    determined.
    */
    static class Reduce4 extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {

            // The decimal value will be rounded and then truncated to 5 decimal
            // places in order to facilitate proper comparison.


			ArrayList<Long> others = new ArrayList<Long>();
            String tmp[];
            String val;
            long col;
            for (Text colAndValue : values) {
                tmp = colAndValue.toString().split(":");
                col = Long.parseLong(tmp[0]);
                val = tmp[1];

                double d = round(Double.parseDouble(val), 3); // rounds to 5 decimal places
                
                if (d>0) {
                	others.add(col);
                }
                
                /*if (hashtable.containsKey(d)) {
                    // update the list in the hashtable:
                    ArrayList<Long> list = hashtable.get(d);
                    if (!list.contains(col))
                        list.add(col);

                }
                else {
                    // create a new list in the hashtable:
                    ArrayList<Long> list = new ArrayList<Long>();
                    if (!list.contains(col))
                        list.add(col);
                    hashtable.put(d, list);
                }*/
            }

            // At this point, we have picked up all the clusters from this row.
            // Send them to the output:
            Collections.sort(others);

            String cluster = "";
            for (int i = 0; i < others.size() - 1; i++) {
                cluster += others.get(i) + ", ";
            }
            cluster += others.get(others.size() - 1);
            context.write(new Text("Cluster:"), new Text(cluster));

		}

        /**
        A simple function that rounds a decimal to the specified number of places.
        Adapted from:
        http://stackoverflow.com/questions/2808535/round-a-double-to-2-decimal-places
        */
        public double round(double value, int places) {
            long factor = (long) Math.pow(10, places);
            value = value * factor;
            long tmp = Math.round(value);
            return (double) tmp / factor;
        }
	}


    /**
    Map 5.
    Part of an optional MR stage that removes duplicate clusters that were
    produced by the previous MR stage.
    */
    static class Map5 extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String str1[] = value.toString().split("\t");
            // This is essentially an identity mapper.
            context.write(new Text(str1[1]), new Text("1"));
        }
    }

    /**
    Reduce 5.
    */
    static class Reduce5 extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
            // This is essentially an identity reducer.
            // To help us with identification of individual clusters, we use
            // the standard Object.getHashCode() function to compute an integer
            // value for each cluster.
            long hashCode = key.toString().hashCode();
            String clusterName = "Cluster ID:" + hashCode;
            context.write(new Text(clusterName), new Text(key.toString()));
		}
	}


    /**
    Map 6.
    This is the last mapper, which compares the clusters against the original
    input file and determines which of those edges are part of the clusters.
    Therefore, the final output will be comparable to that produced by the BC
    algorithm.

    This mapper's input will consist of both the original edge list and the
    output of MR5. The algorithm is very simple. Supposed a given cluster has
    k elements. If the input is a cluster, the mapper sends all k-choose-2
    possible edges from this cluster to various reducers.
     If the input is an edge (from the original input), the mapper simply sends
     that to its corresponding reducer. Whenever a reducer receives 2 copies
     of the same edge, it outputs one copy of that edge.
    */
    static class Map6 extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String s = value.toString();
            if (s.contains("Cluster ID:")) {
                // The input value is a cluster from the previous MR stage.
                // Produce all possible combinations of edges:

                String str[] = s.split("\t");

                // get the hashCode passed on from the previous MR stage
                String hashCode = (str[0].split(":"))[1];

                String nodes[] = str[1].split(", ");
                for (int i = 0; i < nodes.length - 1; i++) {
                    for (int j = i; j < nodes.length; j++) {
                        outputEdge(nodes[i], nodes[j], ("" + hashCode), context);
                    }
                }
            }

            else {
                // Simply send the edge to a reducer
                String str[];
                if (s.contains(" "))
                    str = s.split(" ");
                else
                    str = s.split("\t");

                outputEdge(str[0], str[1], "0:0", context);
            }
        }

        /**
        Helper method that sends the specified edge to the appropriate reducer.
        If an edge from a cluster is specified, the parameter 'String s' should
        be the hashCode of that cluster.
        If a real edge (from the original graph) is specified, the parameter
        'String s' should be marked using some special character to distinguish
        it.
        */
        private void outputEdge(String v1, String v2, String s, Context context) throws java.io.IOException, InterruptedException {
            if (v1.compareTo(v2) < 0)
                context.write(new Text(v1 + "\t" + v2), new Text(s));
            else
                context.write(new Text(v2 + "\t" + v1), new Text(s));
        }
    }


    /**
    Reduce 6.
    This reducer gets either 1 or 2 values for each key.
    Each key is an edge and the values are either "1" and/or a hashCode.
    If 2 values are received, then output this edge.
    */
    static class Reduce6 extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {

            ArrayList<String> strings = new ArrayList<String>();
            for (Text t:values)
                strings.add(t.toString());

            if (strings.size() == 2) {
                String hashCode;
                if (strings.get(0).contains(":"))
                    hashCode = strings.get(1);
                else
                    hashCode = strings.get(0);

                context.write(new Text(key.toString()), new Text(hashCode));
            }

        }
    }
}
