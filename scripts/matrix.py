from sys import argv
import math

filename, inputname = argv

infile = open(inputname, 'r')

matrix = [[0 for _ in range(10)] for _ in range(10)]

for line in infile:
	index = map(int, line.split()[:2])
	value = float("%0.2f" % math.pow(2.718, -1 * float(line.split()[2])))

	if index[0] != index[1]:
		matrix[index[0]][index[1]] = value
		matrix[index[1]][index[0]] = value
print matrix
