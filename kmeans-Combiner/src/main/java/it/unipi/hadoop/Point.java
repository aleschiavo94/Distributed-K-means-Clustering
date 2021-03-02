package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static java.lang.StrictMath.sqrt;

public class Point implements Writable {
	private int dim;
	private double[] coordinates;

	public Point() {
	}

	// costruttore da array di double
	public Point(double[] coordinates) {
		this.dim = coordinates.length;
		this.coordinates = new double[this.dim];
		for (int i = 0; i < coordinates.length; ++i)
			this.coordinates[i] = coordinates[i];
	}

	// costruttore da array di stringhe
	public Point(String[] coordinates) {
		this.dim = coordinates.length;
		this.coordinates = new double[this.dim];
		for (int i = 0; i < coordinates.length; ++i) {
			this.coordinates[i] = Double.parseDouble(coordinates[i]);
		}
	}

	public void setCoordinates(double[] coordinates) {
		for (int i = 0; i < coordinates.length; ++i)
			this.coordinates[i] = coordinates[i];
	}

	public double[] getCoordinates() {
		return this.coordinates;
	}

	public void readFields(DataInput in) throws IOException {
		int nParams = in.readInt();
		this.coordinates = new double[nParams];
		for (int i = 0; i < nParams; ++i)
			this.coordinates[i] = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.coordinates.length);
		for (double elem : coordinates)
			out.writeDouble(elem);
	}

	public double computeDistance(Point point) {
		double result = 0.0;
		double[] diffs = new double[point.dim];
		for (int i = 0; i < point.dim; ++i)
			diffs[i] = this.coordinates[i] - point.coordinates[i];

		for (double elem : diffs)
			result = result + elem * elem;

		return sqrt(result);
	}

	@Override
	public String toString() {
		String output = "";
		for (double c : this.coordinates)
			output += Double.toString(c) + " ";
		return output;
	}

}