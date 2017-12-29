from BTrees.OOBTree import OOBTree
#from btree import BPlusTree
import cPickle
import psutil
import time
import sys

class treeGroup():
	def __del__(self):
		print "treeGroup DELETED"
	def __init__(self,FileName,AttributeName,Keys = OOBTree()):
		self.FileName = FileName
		self.AttributeName = AttributeName
		self.Keys = Keys

def createDiskTree(d,fileName,attributeName):
	print ("btreez.py")
	print (psutil.virtual_memory())
	print ('SAVING B + TREE for {} {}'.format(fileName,attributeName))
	btree = OOBTree()
	btree.update(d)
	start = time.time()
	pointercount = 0
	keycount = 0
	end = time.time()
	f = open('Indexes/' + fileName + '_'+ attributeName +'.btree', 'wb')
	sys.setrecursionlimit(30000)
	cPickle.dump(btree,f,protocol=cPickle.HIGHEST_PROTOCOL)
	print('BTree saved to disk.')

def createMemoryTree(d,fileName,attributeName):
	print ("btreez.py")
	print (psutil.virtual_memory())
	print ('LOADING INTO MEMORY B + TREE for {} {}'.format(fileName,attributeName))
	btree = OOBTree()
	btree.update(d)
	t = treeGroup(fileName,attributeName,btree)
	print('BTree stored in memory')
	return t


