from sys import argv

filename, inputfilename, outputfilename = argv

infile = open(inputfilename, 'r')
outfile = open(outputfilename, 'w')

i = 0
for line in infile:
	outfile.write(str(i) + " " + line)
	if i%1000==0:
		print i
	i+=1

infile.close()
outfile.close()
