from BTrees.OOBTree import OOBTree
#from btree import BPlusTree
import cPickle
import time
def loadTree(fileName,attributeName):
	f = open('Indexes/' + fileName + '_'+ attributeName +'.btree', 'rb')
	bt = cPickle.load(f)
	#bt = OOBTree()
	#bt.update(bt_dict)
	return bt
	#start = time.time()
	#result =  {key:val for key,val in bt.items() if (key >= 'H3w5XeX-OO0Wk13fwLOybQ' and key <= 'xxjxUM-VK4N33LN8NehN-w')}
	#end = time.time()
	#raw_input ("Time taken to retrive pointers from disk: {}".format(end - start))
	#for key,val in bt.items():
	#	if (key >= 'H3w5XeX-OO0Wk13fwLOybQ' and key <= 'xxjxUM-VK4N33LN8NehN-w'):
	#		raw_input("{} {}".format(key,val))
	#print (result)
	#print ("btreez_load.py")


	#raw_input('press Enter to finish')
