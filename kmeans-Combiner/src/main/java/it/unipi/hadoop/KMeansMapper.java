package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 10,15,30,50
// 20,30,50,90
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
	Point newPoint;
	IntWritable clusterIndex;
	Point[] centers;

	public void setup(Context context) throws IOException, InterruptedException {
		String[] centersString;
		centersString = context.getConfiguration().getStrings("cluster.centers");
		centers = new Point[centersString.length];

		// getting centers
		for (int i = 0; i < centersString.length; ++i) {
			String[] centerCoords = centersString[i].split(" ");
			centers[i] = new Point(centerCoords);
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		newPoint = new Point(value.toString().split(" "));
		Double min = Double.MAX_VALUE;
		int centersNumber = centers.length;
		for (int i = 0; i < centersNumber; i++) {
			Double dist = newPoint.computeDistance(centers[i]);
			if (dist < min) {
				min = dist;
				clusterIndex = new IntWritable(i);
			}
		}
		context.write(clusterIndex, newPoint);
	}
}
