from sys import argv
import math

filename, inputfilename, outputfilename = argv

lines = [line for line in open(inputfilename, 'r')]
output = open(outputfilename, 'w')

maxvalues = [2076, 0, 0, 72564, 45698, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

for i,stringI in enumerate(lines):
	print i
	for j,stringJ in enumerate(lines):
		idI = stringI.split(' ')[0]
		valuesI = stringI.split(' ')[1].split(',')

		idJ = stringJ.split(' ')[0]
		valuesJ = stringJ.split(' ')[1].split(',')

		distance = 0

		for k in range(17):
			if k in [1,2]:
				if valuesI[k] != valuesJ[k]:
					distance += 1
			else:
				distance += math.fabs(float(valuesI[k]) - float(valuesJ[k])) / maxvalues[k]

		weight = math.exp(-1 * distance)
		output.write(str(i) + ' ' + str(j) + ' ' + str(weight) + '\n')
