from sys import argv, maxint

filename, inputfilename = argv

lines = [line for line in open(inputfilename, 'r')]
biggest = [0 for i in range(41)]
smallest = [maxint for i in range(41)]

k=0
for i in lines:
	if k%10000 == 0:
		print k
	values = i.split(',')[:-1]
	for j in range(41):
		if not j in [1,2,3]: 
			if float(values[j]) < smallest[j]:
				smallest[j] = float(values[j])
			if float(values[j]) > biggest[j]:
				biggest[j] = float(values[j])
	k+=1

for i in range(41):
	print "Biggest: ", biggest[i], '\t', "Smallest: ", smallest[i]

