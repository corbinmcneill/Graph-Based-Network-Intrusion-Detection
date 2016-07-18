from sys import argv

def find(vertex, disjointSets):
	for i in xrange(len(disjointSets)):
		if vertex in disjointSets[i]:
			return i
	assert False

def join(set1, set2, disjointSets):
	if set1 == set2:
		return disjointSets
	if set2 < set1:
		temp = set2
		set2 = set1
		set1 = temp
	dset = disjointSets.pop(set2)
	disjointSets[set1] += dset
	return disjointSets

def componentcount(inputfilename, size):
	lines = [line for line in open(inputfilename,'r')]
	disjointSets = [[i] for i in xrange(int(size))]

	while len(lines) > 0:
		line = lines.pop(0);

		vertex1, vertex2, weight = line.split(',')
		vertex1 = int(vertex1)
		vertex2 = int(vertex2)

		disjointSets = join(find(vertex1, disjointSets), find(vertex2, disjointSets), disjointSets)

	result = ""
	for disjointSet in disjointSets:
		result += ' '.join(map(str,sorted(disjointSet)))+"\n\n"
	return result

if __name__ == "__main__":
	filename, inputfilename, size = argv
	print componentcount(inputfilename, size)

