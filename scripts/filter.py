from sys import argv, maxint

filename, inputfilename, outputfilename = argv

lines = [line for line in open(inputfilename, 'r')]
outfile = open(outputfilename, 'w')

k=0
for i in lines:
	if k%1000 == 0:
		print k
	k+=1
	values = i.split(',')[:-1]
	if values[11] == '1':
		outfile.write(i)
		


