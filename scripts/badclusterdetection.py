from sys import argv

filename, clustername, dataname, threshold = argv

clusterfile = open(clustername, 'r')
datafile = open(dataname, 'r')
threshold = float(threshold)

# process data file to determine which entries are normal
# and which are anamolous 
data = {}
line = datafile.readline()
while line != '':
	cleanlinesplit = line.rstrip().split(' ')
	data[cleanlinesplit[0]] = cleanlinesplit[1].split(',')[-1]
	line = datafile.readline()

# read clusters from clusterfile
totalVertices = 0
clusters = []
line = clusterfile.readline()
while line != '':
	if line != '\n':
		clusters.append(line)
		totalVertices += len(line.split(' '))
	line = clusterfile.readline()

# analyze clusters
tp = 0
tn = 0
fp = 0
fn = 0
positiveClusters = 0
negativeClusters = 0
for cluster in clusters: 
	vertices = cluster.rstrip().split(' ')
	if len(vertices) > threshold * totalVertices:
		positiveClusters += 1
		for vertex in vertices:
			if data[vertex] == "normal.":
				tp += 1
			else:
				fp += 1
	else:
		negativeClusters += 1
		for vertex in vertices:
			if data[vertex] == "normal.":
				fn += 1
			else:
				tn += 1

#print analysis
print "Positive Clusters:", str(positiveClusters)
print "Negative Clusters:", str(negativeClusters)
print "TP:               ", str(tp)
print "TN:               ", str(tn)
print "FP:               ", str(fp)
print "FN:               ", str(fn)
print "TPR:              ", str(float(tp) / (tp + fp))
print "TNR:              ", str(float(tn) / (tn + fn))
print "FPR:              ", str(float(fp) / (tp + fp))
print "FNR:              ", str(float(fn) / (tn + fn))
print "ACCURACY:         ", str((tp + tn) / float(tp + tn + fp + fn))
