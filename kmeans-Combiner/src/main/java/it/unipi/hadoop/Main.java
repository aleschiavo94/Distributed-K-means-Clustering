package it.unipi.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;

import static it.unipi.hadoop.buildCenters.extractCenters;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: KMeans <inputFile> <nClusters> <outputFolder>");
			System.exit(2);
		}

		System.out.println("args[0]: <inputFile> = " + otherArgs[0]);
		System.out.println("args[1]: <nClusters> = " + otherArgs[1]);
		System.out.println("args[2]: <outputFolder> = " + otherArgs[2]);

		Integer iteration = 1;
		Integer nCluster = Integer.parseInt(otherArgs[1]);
		String[] newCenterValues = new String[nCluster];
		
		// extracting random initial Centers
		String[] centerValues = new String[nCluster];
		int i_center_counter = 0;
		int o_center_counter = 0;
		
		centerValues = extractCenters(nCluster, otherArgs[0]);

		// initializing centers
		for (int i = 0; i < centerValues.length; ++i)
			newCenterValues[i] = centerValues[i];
		
		do {
			Job job = Job.getInstance(conf, "KMeans");
			job.setJarByClass(Main.class);
	
			// set Mapper/Reducer/Combiner
			job.setMapperClass(KMeansMapper.class);
		    job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReducer.class);
	
			// define mapper's output key-value
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Point.class);
	
			// define reducer's output key-value
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Point.class);
	
			// set number of cluster wanted
			job.getConfiguration().setInt("kMeans.numOfCluster", nCluster);
			job.getConfiguration().setStrings("cluster.centers", newCenterValues);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(new StringBuilder().append(otherArgs[2]).append(iteration).toString()));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.waitForCompletion(true);

			String[] printCenters = job.getConfiguration().getStrings("cluster.centers");

			for (String s : printCenters) {
				System.out.println("[Input Center #" + i_center_counter + "]: " + s);
				i_center_counter++;
			}
			i_center_counter = 0;
			System.out.println("\n");

			String dir_output = otherArgs[2] + iteration;
			//System.out.println("DIR_OUTPUT: " + dir_output + "\n\n");
			ByteArrayOutputStream byte1 = new ByteArrayOutputStream();
			PrintStream out2 = new PrintStream(byte1);

			FileSystem fs = FileSystem.get(new Path(new StringBuilder().append(dir_output).append(iteration - 1).toString()).toUri(), conf);
			FSDataInputStream in = null;

			// read new centers from last output file
			try {
				in = fs.open(new Path(new StringBuilder().append(dir_output).append("/part-r-00000").toString()));
				IOUtils.copyBytes(in, out2, 4096, false);
				String s = byte1.toString();
				String lines[] = s.split("\n");

				for (String str : lines) {
					//System.out.println("PRINT LINES: " + str);
					String[] coord = str.replaceAll("\\s+", " ").split(" ");
					Integer index = Integer.parseInt(coord[0]);
					newCenterValues[index] = "";
					for (int i = 1; i < coord.length; ++i) 
						newCenterValues[index] += coord[i] + " ";
					
					System.out.println("[Output Center #" + o_center_counter + "]: " + newCenterValues[index]);
					o_center_counter++;
				}
				System.out.println("\n");
				o_center_counter = 0;

			} finally {
				IOUtils.closeStream(in);
			}

			if (checkStopCondition(centerValues, newCenterValues)) {
				System.out.println("STOPPING CONDITION REACHED.");
				break;
			} else {
				for(int i = 0; i < newCenterValues.length; ++i)
					centerValues[i] = newCenterValues[i];
			}
			
			System.out.println("ITERATION NUMBER: " + iteration + "\n");
			iteration++;
		} while (iteration < 20);

		if (iteration >= 20)
			System.out.println("MAX ITERATIONS REACHED.");
	}

	public static Boolean checkStopCondition(String[] oldCenters, String[] newCenters) {
		Point oldCentroid;
		Point newCentroid;
		double delta = 30;
		double diff;
		boolean exitFlag = true;
		for (int i = 0; i < oldCenters.length; ++i) {
			oldCentroid = new Point(oldCenters[i].split(" "));
			System.out.println("INPUT CENTER:" + oldCentroid.toString());
			newCentroid = new Point(newCenters[i].split(" "));
			System.out.println("OUTPUT CENTER:" + newCentroid.toString());
			diff = oldCentroid.computeDistance(newCentroid);
			System.out.println("Distance: " + diff + "\n");
			if (diff > delta)
				exitFlag = false;
		}
		return exitFlag;
	}
}
