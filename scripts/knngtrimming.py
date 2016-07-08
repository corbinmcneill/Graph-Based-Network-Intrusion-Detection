from sys import argv
import math

filename, inputfilename, outputfilename, knng = argv

knng=int(knng)
lines = [line for line in open(inputfilename, 'r')]
output = open(outputfilename, 'w')

weights = {}
for line in lines:
	i, j, weight = line.split()
	if i in weights:
		weights[i][j] = weight
	else:
		weights[i] = {j:weight} 
	
	if j in weights:
		weights[j][i] = weight
	else:
		weights[j] = {i:weight}

alreadyAdded = []
for index in weights:
	for keep in sorted(weights[index].iterkeys(), key=(lambda key: weights[index][key]))[::-1][:knng]:
		if not ' '.join(keep.split(' ')[::-1]) in alreadyAdded:
			output.write(index + " " +  keep + " " + str(weights[index][keep]) + "\n")
			alreadyAdded.append(keep)

output.close()
