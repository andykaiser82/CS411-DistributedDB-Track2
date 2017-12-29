import itertools
import collections
from sys import getsizeof
import sys
#from btree import *
import time
def sortData(vals):
	print "SORTED!"		
	s = list(k for k,_ in itertools.groupby(sorted(vals)))
	count  = 1
	for i in s:
		print "{} {}".format(count,i)
		count +=1
	print "Total {}".format(count-1)

def sortData2(d,fileName,attributeName):
	indexFile = "Indexes/key_value_indeces/"+ fileName +"/"+ fileName + '_'+ attributeName +".kvi"
	print ("OUTPUTTING DICTIONARY TO FILE: "+ indexFile)
	out = open(indexFile,"wb")
	sys.stdout = out
	#print vals
	
	#s = collections.OrderedDict(sorted(d.items()))
	
	count  = 1
	for k, v in d.iteritems():
		print "{} {}".format(k,v)
		count +=1
	out.close()
	sys.stdout = sys.__stdout__
	print ("Total {}".format(count-1))

def getValues(d):
	print "GETTING VALUES"
	start = time.time()
	print d['gae9LAyt7Qvf_OgAkWASxA']
	end = time.time()
	print (end - start)

