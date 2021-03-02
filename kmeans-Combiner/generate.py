from random import uniform
import sys
import math

# num_elements, dim_span, range 

def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

args = sys.argv
dim_range = int(args[3])
dimension = int(args[2])
n_points = int(args[1])
string = ""

file = open("data_set_.txt", "w")

for k in range(n_points):
	for e in range(dimension):
		e = truncate(float(uniform(-dim_range, dim_range)), 3);
		string += str(e) + ' '
	string += '\n'
	file.write(string)
	string = ""
	
file.close()