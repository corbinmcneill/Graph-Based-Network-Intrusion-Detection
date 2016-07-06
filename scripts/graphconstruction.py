from sys import argv
import math

filename, inputfilename, outputfilename = argv

lines = [line for line in open(inputfilename, 'r')]
output = open(outputfilename, 'w')

maxvalues = [58329, 0, 0, 0, 693375640, 5155468, 1, 3, 3, 30, 5, 1, 884, 1, 2, 993, 28, 2, 8, 1, 1, 1, 511, 511, 1, 1, 1, 1, 1, 1, 255, 255, 1, 1, 1, 1, 1, 1, 1, 1]

for i,stringI in enumerate(lines):
	print i
	for j,stringJ in enumerate(lines):
		if j%100 == 0:
			print j
		idI = stringI.split(' ')[0]
		valuesI = stringI.split(' ')[1].split(',')

		idJ = stringJ.split(' ')[0]
		valuesJ = stringJ.split(' ')[1].split(',')

		distance = 0

		for k in range(40):
			if k in [1,2,3]:
				if valuesI[k] != valuesJ[k]:
					distance += 1
			else:
				distance += math.fabs(float(valuesI[k]) - float(valuesJ[k])) / maxvalues[k]

		weight = math.exp(-1000 * distance)
		output.write(str(i) + ' ' + str(j) + ' ' + str(weight))
