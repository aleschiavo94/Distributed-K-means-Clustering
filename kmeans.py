import random
import math
import sys

from pyspark import SparkContext

class Point:
    
    def __init__(self, dimension, coordinates):
            self.dimension = int(dimension)
            self.coordinates = []
       	    for x in range(self.dimension):
	              self.coordinates.append(float(coordinates[x]))
   		    
    def getVal(self, index):
        return self.coordinates[index]

    def printPoint(self):
        for x in range(self.dimension):
            print(self.coordinates[x])
            
    def printWhole(self):
        result = ""
        for x in range(self.dimension):
            result = result + str(self.coordinates[x])
            if(x < (self.dimension)-1):
                result = result + " "
        return result
    
	# calculation of the distance between two points
    def computeDistance(self, otherPoint):
        result = 0.0
        diffs = []
        for x in range(0, self.dimension):    	
            diffs.append(float(self.coordinates[x]) -  float(otherPoint.getVal(x)))
        
        for x in diffs:
            result = result + x * x
        
        return math.sqrt(result)

# calculation of file length
def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# random extraction of centers
def extractCenters(numberOfCenters, file, dimension):
    filelen = file_len(file)
    centroidIndexes = []
    centroidIndexes = random.sample(range(0, filelen), numberOfCenters)

    file = open(file, "r")
    points = []
    i = 0
    set = range(0, filelen)
    for count in set:
        line = file.readline()
        if(centroidIndexes.__contains__(i)):
            split = line.split(" ")
            p1 = Point(dimension, split)
            points.append(p1)
        i+=1
    file.close()
    return points
 
# calculation of the nearest centroid 
def closestCentroid(line, dimension, centers):
    point = Point(dimension, line)
    minim = sys.maxsize
    for x in range(centers.__len__()):
        dist = point.computeDistance(centers[x])
        if(dist < minim):
            minim = dist
            clusterIndex = x
        
    return clusterIndex

# calculation of the averange distance
def avg(pointList):
    totalSum = []
    i = 0
    for point in pointList:
        if i==0:   
            split = point.split(" ")
            counter = 0
            for coordinate in split:
                totalSum.append(float(coordinate))
                counter+=1
            i+=1
        else:
            split = point.split(" ")
            counter = 0
            for coordinate in split:
                totalSum[counter] += (float(coordinate))
                counter+=1
            i+=1
            
    for x in range(0, totalSum.__len__()):
        totalSum[x] = totalSum[x] / i
        totalSum[x] = round(totalSum[x], 3)
    return totalSum

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: kmeans [number_of_centers] [points_dimension] [input_file] [delta_convergence]", file=sys.stderr)
        sys.exit(-1)

    numberOfCenters = int(sys.argv[1])
    dimension = int(sys.argv[2])
    inputDataset = sys.argv[3]
    delta = float(sys.argv[4])

    def checkStopCondition(centerDistances):
        for x in centerDistances:
            if x > delta:
                return False
        return True

    # extracting intial k random centers 
    centers = extractCenters(numberOfCenters, inputDataset, dimension)

    # print randomly chosen centers 
    center_num = 0
    for x in centers:
        print("[Initial Center #" + str(center_num) + "]:")
        print(x.printWhole())
        center_num += 1
    
    sc = SparkContext(appName = "k-means",  master = "yarn")
    points = sc.textFile(inputDataset).cache()

    iteration = 1
 
    while True:
        # map each point to the closest cluster 
        mapped = points.map(lambda x: (closestCentroid(x.split(" "), dimension, centers), Point(dimension, x.split(" ")).printWhole()))

        # obtain [cluster, list_of_members] RDD
        clusters = mapped.groupByKey()

        # for each cluster, compute the center as the avg of member points 
        clusterCenters = clusters.mapValues(avg).sortByKey()

        # collect computed centers into a list 
        currentCenters = clusterCenters.collect()

        newCenters = []
        centerDistances = []
        
        # update center values 
        for x in range(len(centers)):
            for c in range(len(currentCenters)):
                if(x == currentCenters[c][0]):
                    newCenters.append(Point(dimension, currentCenters[c][1]))

        # compute distances between old centers and new centers 
        for x in range(len(centers)):
            centerDistances.append(centers[x].computeDistance(newCenters[x]))
      
        print("\n\n[computed distances]:")            
        print(centerDistances)
        print("\n\n")

        # check if convergence has been reached 
        if (checkStopCondition(centerDistances)):
            break

        centers = newCenters
        iteration += 1

    clusterCenters.saveAsTextFile("output")
    print("\n\n[# Iteration performed]: " + str(iteration) + "\n\n")