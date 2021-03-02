from random import uniform
import sys
import math

# num_elements, dim_span, range 

def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

if len(sys.argv) < 3:
    print("Usage: generate.py [number_of_points] [points_dimension] [range]", file=sys.stderr)
    sys.exit(-1)


args = sys.argv
dim_range = int(args[3])
dimension = int(args[2])
n_points = int(args[1])
string = ""

file = open("data_set_.txt", "w")

for k in range(n_points):
	for e in range(dimension):
		e = float('%.3f'%(uniform(1650, 2450)))
		string += str(e) + ' '
	string += '\n'
	file.write(string)
	string = ""
	
file.close()