package it.unipi.hadoop;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

    public void reduce(IntWritable clusterIndex, Iterable<Point> clusterMembers, Context context)
            throws IOException, InterruptedException {

        double[] sumPointArrR;
        Point sumPoint;

        sumPointArrR = combine(clusterMembers);

        //sumPoint = [SumCoord1, SumCoord2, SumCoord3, numPointSummed]
        sumPoint = new Point(sumPointArrR);
        context.write(clusterIndex, sumPoint);
    }

    public double[] combine(Iterable<Point> points) {

        Point point;
        int numberOfPoints = 0;
        int dimPoint = 0;
        double[] pointCoords;
        double[] sumPointCoordinates;

        // getting # of coordinates
        if (points.iterator().hasNext()) {
            point = points.iterator().next();
            numberOfPoints++;
            pointCoords = point.getCoordinates();
            dimPoint = pointCoords.length + 1;
            sumPointCoordinates = new double[dimPoint];
            for (int i = 0; i < dimPoint - 1 ; ++i) {
                sumPointCoordinates[i] += pointCoords[i];
            }

            // save sum of coordinates of all points
            while (points.iterator().hasNext()) {
                point = points.iterator().next();
                numberOfPoints++;
                pointCoords = point.getCoordinates();
                for (int i = 0; i < dimPoint - 1; ++i) {
                    sumPointCoordinates[i] += pointCoords[i];
                }
            }

            //adding # of points summed in last pos
            sumPointCoordinates[dimPoint - 1] = numberOfPoints;
            return sumPointCoordinates;
        } else {
            System.err.println("Empty list of points");
            return null;
        }
    }
}
