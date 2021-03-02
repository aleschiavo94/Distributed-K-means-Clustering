package it.unipi.hadoop;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// public class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Point> {
public class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Point> values, Context context)
			throws IOException, InterruptedException {

		Point newCenter;
		Text output = new Text();
		newCenter = computeAverage(values);
		output.set(newCenter.toString());
		context.write(key, output);
	}

	public Point computeAverage(Iterable<Point> points) {

		Point avgPoint;
		Point point;
		int numberOfPoints = 0;
		int dimPoint = 0;
		double[] pointCoords;
		double[] avgPointCoordinates;
		double[] result;

		// getting # of coordinates
		if (points.iterator().hasNext()) {
			point = points.iterator().next();
			pointCoords = point.getCoordinates();
			dimPoint = pointCoords.length;
			avgPointCoordinates = new double[dimPoint];
			result = new double[dimPoint - 1];
			for (int i = 0; i < dimPoint; ++i) {
				avgPointCoordinates[i] += pointCoords[i];
			}

			// save sum of coordinates of all points
			while (points.iterator().hasNext()) {
				point = points.iterator().next();
				pointCoords = point.getCoordinates();
				for (int i = 0; i < dimPoint; ++i) {
					avgPointCoordinates[i] += pointCoords[i];
				}

			}

			// save # of points summed
			numberOfPoints += avgPointCoordinates[dimPoint - 1];

			// compute average
			for (int i = 0; i < dimPoint - 1; ++i) {
				// save avg and discard last element which contains numberOfPoints
				result[i] = avgPointCoordinates[i] / numberOfPoints;
			}

			avgPoint = new Point(result);
			return avgPoint;
		} else {
			System.err.println("Empty list of points");
			return null;
		}
	}
}
