from sys import argv 
from random import randint
from string import strip


filename, inputname, outputname, size = argv
lines = [line for line in open(inputname, 'r')]
outfile = open(outputname, 'w')

NUMBER_OF_SAMPLES = int(size)
DIMENSIONS = [0, 2, 3, 4, 5, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 41]

attackTypes = {}
maximums = [0 for _ in xrange(len(DIMENSIONS))]
k = 0
while k<NUMBER_OF_SAMPLES:
	if k%1000 == 0:
		print k
	k+=1
	index = randint(0,len(lines)-1)
	item = lines.pop(index)

	if ' ' in item:
		line = item.split(' ')[1]
	else:
		line = item

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


	outfile.write(str(k) + ' ' + ','.join(datum) + '\n')

outfile.close()
print
if 41 in DIMENSIONS:
	print "MAXIMUMS:\t" + str(map(int,maximums[:-1]))
else:
	print "MAXIMUMS:\t" + str(maximums)
print "ATTACK TYPES:\t" + str(attackTypes) 
print
