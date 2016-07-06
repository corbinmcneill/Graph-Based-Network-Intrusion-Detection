from sys import argv
import math

filename, inputfilename, outputfilename, knng = argv

knng=int(knng)
lines = [line for line in open(inputfilename, 'r')]
output = open(outputfilename, 'w')

maxvalues = [58329, 0, 0, 0, 693375640, 5155468, 1, 3, 3, 30, 5, 1, 884, 1, 2, 993, 28, 2, 8, 1, 1, 1, 511, 511, 1, 1, 1, 1, 1, 1, 255, 255, 1, 1, 1, 1, 1, 1, 1, 1]

alreadyAdded = []

for i,stringI in enumerate(lines):
	print i
	weights = {}
	for j,stringJ in enumerate(lines):
		if i==j:
			continue
		idI = stringI.split(' ')[0]
		valuesI = stringI.split(' ')[1].split(',')

		idJ = stringJ.split(' ')[0]
		valuesJ = stringJ.split(' ')[1].split(',')

		distance = 0

		for k in range(len(valuesI)-1):
			if k in [1,2,3]:
				if valuesI[k] != valuesJ[k]:
					distance += 1
			else:
				distance += math.fabs(float(valuesI[k]) - float(valuesJ[k])) / maxvalues[k]

		weight = math.exp(-1000 * distance)
		weights[str(i) + " " + str(j)] = weight
	for keep in sorted(weights.iterkeys(), key=(lambda key: weights[key]))[::-1][:knng]:
		if not ' '.join(keep.split(' ')[::-1]) in alreadyAdded:
			output.write(keep + " " + str(weights[keep]) + "\n")
			alreadyAdded.append(keep)

output.close()
