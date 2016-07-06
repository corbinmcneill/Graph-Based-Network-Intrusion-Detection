from sys import argv
import math

filename, inputfilename, outputfilename, knng = argv

knng=int(knng)
lines = [line for line in open(inputfilename, 'r')]
output = open(outputfilename, 'w')

weights = {}
for line in lines:
	i, j, weight = line.split(" ")
	weights[i + " " + j] = float(weight)

alreadyAdded = []
for keep in sorted(weights.iterkeys(), key=(lambda key: weights[key]))[::-1][:knng]:
	if not ' '.join(keep.split(' ')[::-1]) in alreadyAdded:
		output.write(keep + " " + str(weights[keep]) + "\n")
		alreadyAdded.append(keep)

output.close()
