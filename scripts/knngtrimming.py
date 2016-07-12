from sys import argv
import math

filename, inputfilename, outputfilename, knng = argv

knng=int(knng)
lines = [line for line in open(inputfilename, 'r')]
output = open(outputfilename, 'w')

weights = {}
for line in lines:
	i, j, weight = line.split()
	i = int(i)
	j = int(j)

	if i < j:
		v = [i,j]
	else:
		v = [j,i]

	if v[0] in weights:
		weights[v[0]][v[1]] = weight
	else:
		weights[v[0]] = {v[1]:weight} 

alreadyAdded = []
for a in weights:
	for b in sorted(weights[a].iterkeys(), key=(lambda key: weights[a][key]))[::-1][:knng]:
		if a < b:
			v = [a,b]
		else:
			v = [b,a]
		if not ' '.join(map(str,v)) in alreadyAdded:
			output.write(' '.join(map(str,v)+[weights[v[0]][v[1]]]) + '\n')
			alreadyAdded.append(' '.join(map(str,v)))

output.close()
