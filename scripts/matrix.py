from sys import argv
import math

filename, inputname, size = argv

infile = open(inputname, 'r')

matrix = [[1 for _ in range(int(size))] for _ in range(int(size))]

for line in infile:
	index = map(int, line.split(',')[:2])
	value = "%0.2f" % float(line.split(',')[2])

	if index[0] != index[1]:
		matrix[index[0]][index[1]] = 1
		matrix[index[1]][index[0]] = 1
print matrix
