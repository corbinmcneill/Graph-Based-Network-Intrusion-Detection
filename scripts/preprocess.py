from sys import argv 
from random import randint
from string import strip

NUMBER_OF_SAMPLES = 500
DIMENSIONS = [0, 2, 3, 4, 5, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 41]


if len(argv) != 3:
	print "Invalied parameters. Please run this script as \n\tpython preprocess.py <inputfile> <outputfile>\n\nClosing script now."
	exit(1)

filename, inputname, outputname = argv
lines = [line for line in open(inputname, 'r')]
outfile = open(outputname, 'w')

previousIndexes = []
attackTypes = {}
maximums = [0 for _ in xrange(len(DIMENSIONS))]
while len(previousIndexes)<NUMBER_OF_SAMPLES:
	index = randint(0,len(lines)-1)
	if index in previousIndexes:
		continue
	i = len(previousIndexes)
	previousIndexes.append(index)

	if ' ' in lines[index]:
		line = lines[index].split(' ')[1]
	else:
		line = lines[index]

	datum = line.split(',')
	for j in range(len(datum))[::-1]:
		if j is 41:
			datum[j] = datum[j][:-1]
			if datum[j] in attackTypes:
				attackTypes[datum[j]] = attackTypes[datum[j]] + 1
			else: 
				attackTypes[datum[j]] = 1
		if j in DIMENSIONS:
			if not j in [1,2,3,41]:
				maximums[DIMENSIONS.index(j)] = max(maximums[DIMENSIONS.index(j)], float(datum[j]))
		else:
			datum.pop(j)


	outfile.write(str(i) + ' ' + ','.join(datum) + '\n')

outfile.close()
print
if 41 in DIMENSIONS:
	print "MAXIMUMS:\t" + str(map(int,maximums[:-1]))
else:
	print "MAXIMUMS:\t" + str(maximums)
print "ATTACK TYPES:\t" + str(attackTypes) 
print
