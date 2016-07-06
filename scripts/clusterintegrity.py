from sys import argv

filename, clustername, dataname = argv

clusterfile = open(clustername, 'r')
datafile = open(dataname, 'r')

data = {}
line = datafile.readline()
while line != '':
	cleanlinesplit = line.rstrip().split(' ')
	data[cleanlinesplit[0]] = cleanlinesplit[1].split(',')[-1]
	line = datafile.readline()

for cluster in clusterfile:
	if cluster == '\n':
		continue
	clusterSize = len(cluster.split(' '))
	clusterInfo = {}
	for point in cluster.split(' '):
		classification = data[point.rstrip()]
		if classification in clusterInfo:
			clusterInfo[classification] += 1
		else:
			clusterInfo[classification] = 1
	
	print "CLUSTER SIZE:", clusterSize 
	for info in clusterInfo:
		print info[:-1]+":", clusterInfo[info]/float(clusterSize)
	print 

