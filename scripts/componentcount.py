from sys import argv

filename, inputfilename = argv

lines = [line for line in open(inputfilename,'r')]

disjointSets = [[i] for i in xrange(500)]

def find(vertex):
	for i in xrange(len(disjointSets)):
		if vertex in disjointSets[i]:
			return i
	assert False

def join(set1, set2):
	if set1 == set2:
		return
	if set2 < set1:
		temp = set2
		set2 = set1
		set1 = temp
	dset = disjointSets.pop(set2)
	for item in dset:
		disjointSets[set1].append(item)

while len(lines) > 0:
	line = lines.pop(0);

	vertex1, vertex2, weight = line.split(',')
	vertex1 = int(vertex1)
	vertex2 = int(vertex2)
	join(find(vertex1), find(vertex2))


for disjointSet in disjointSets:
	print ' '.join(map(str,sorted(disjointSet)))+'\n'
	
