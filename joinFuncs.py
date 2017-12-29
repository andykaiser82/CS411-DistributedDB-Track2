import time
import itertools
from itertools import product
from enum import IntEnum
class operators(IntEnum):
	equals = 1	
	greater_than =2
	greater_or_equal = 3
	less_than = 4
	less_or_equal = 5
	not_equal = 6

def filterOnKeys(tr,ur,at_dict):
	print ("filterOnKeys")	
	#b = set(biggerSet)
	#print("{}\nbig set ".format(b))
	#examine each tupleResult
	#for i in tr:
	#examine each file in tupleResult
	for j in range(0,len(tr.alias)):
		#raw_input("i.alias: {}".format(tr.alias))
			
		b = set()
		#Get the set of rows that are valid for this file name as key
		alias = tr.alias[j]
		#print "ALIAS IS {}".format(alias)
		b = set(ur[alias])
		#raw_input(": {} \nur[alias] {} alias".format(ur[alias],alias))
		#examine each list at the same index as i.fName 
		#raw_input(" {}\nBefore: {}".format(tr.list[j],len(tr.list[j])))	
		for k in reversed(tr.list[j]):
			a = set(k)
		#Get the intersection of the valid rows for this key
			result = a.intersection(b)
			
		#raw_input(" {}\nAfter: {}".format(tr.list[j],len(tr.list[j])))		
			#if len(k)!=len(result) and len(result) > 0:
				#raw_input(" {}\nBefore: {}".format(b,len(k)))
				#raw_input("{}\nAfter: {}".format(result,len(result)))
				#raw_input()
			if tr.list[j][0] == [] or tr.list[j][1] == []:
				del tr.list[j]

	return tr

#Given the complete list of results of two searches, find the items
#that are contained on both lists (Intersection)
def findIntersection(l1,l2):
	a = set(l1)
	b = set(l2)
	result = a.intersection(b)
	return result

def getIndexFromTR(btree,i,fName,attrName):
	if btree[i].FileName == fName and btree[i].AttributeName == attrName:
		#raw_input("returning {}".format(i))
		return i
	else:
		return -1
#Given an argument that wants to check an attribute for equality with 
#a literal condition
def equalCondition(tr,btree,searchkey):
	#raw_input("equal condition")
	b_idx= []
	#find the correct btree that matches that fileName and attribute for each attributeName 
	#in the argument
	for i in range(0,len(btree)):
		#iterate over each file name in the tuple result
		for j in range (0,len(tr.fName)):
			#identify the appropriate index for the btree that matches on the filename
			if j == 0:
				idx = getIndexFromTR(btree,i,tr.fName[j],tr.attribute[j])
				if idx !=-1:
					b_idx.append(idx)
	for i in range(0,2):
		tr.list.append([])

	#create lists of rows that match the attribute's keys for each attribute
	for i in range(0,len(b_idx)):
		tr = compareAttributeToKey(btree,b_idx[i],searchkey,tr,1)

	#Merge the lists created for table 1 and table two
	for i in range(0,len(tr.list)):
		mymerge = []
		mymerge = mergeLists(tr.list[i])
		tr.merged.append(mymerge)
	for i in tr.merged:
		i.sort()

	return tr

def equalityJoin(f1,f2,tr,btree):
	print("equality join")
	b_idx= []
	#find the correct btree that matches that fileName and attribute for each attributeName 
	#in the argument
	#print 'len(btree): ' + str(len(btree))
	for i in range(0,len(btree)):
		#iterate over each file name in the tuple result
		#print 'len(tr.fName): ' + str(len(tr.fName))
		for j in range (0,len(tr.fName)):
			#identify the appropriate index for the btree that matches on the filename
			idx = getIndexFromTR(btree,i,tr.fName[j],tr.attribute[j])
			if idx !=-1:
				b_idx.append(idx)

	print b_idx
	start = time.time()
	mergedrows = []
	for i in range(0,2):
		tr.list.append([])
	emptySet = []
	for key in btree[b_idx[0]].Keys:
		if key in btree[b_idx[1]].Keys:
			tr.key.append(key)
			tr.list[0].append(btree[b_idx[0]].Keys[key])
			tr.list[1].append(btree[b_idx[1]].Keys[key])

	#Merge the lists created for table 1 and table two
	#raw_input("the list length is {}".format(len(tr.list)))
	for i in range(0,len(tr.list)):
		mymerge = []
		mymerge = mergeLists(tr.list[i])
		tr.merged.append(mymerge)
	for i in tr.merged:
		i.sort()
	return tr
#Add the list of rows for any keys that match on a contdition
def compareAttributeToKey(btree,idx,searchkey,tr,flag):
	#raw_input("searchkey is{}".format(searchkey))
	if searchkey in btree[idx].Keys:
		tr.key.append(searchkey)
		if flag ==1:
			#raw_input("{}\nappended to list".format(btree[idx].Keys[searchkey]))
			tr.list[0].append(btree[idx].Keys[searchkey])
		elif flag ==2:
			#raw_input("{}\nappended to list".format(btree[idx].Keys[searchkey]))
			tr.list[1].append(btree[idx].Keys[searchkey])
	else:#return an empty set if the key is not found in the btree
		emptySet = []
		tr.list.append(emptySet)
	return tr
#merges all items in a list of tuple results
def mergeLists(my_list):
	#raw_input("len my_list {}".format(len(my_list)))
	merged = []
	for i in my_list:
		merged+=i
		#merged.append(i)
	#raw_input("TOTAL ROWS {}".format(len(merged)))
	return merged

def showTreeGroups(btree):
	for i in range(0,len(btree)):
		print 'treeGroup #' + str(i)
		print 'FileName: ' + btree[i].FileName
		print 'AttributeName: ' + btree[i].AttributeName

#def non_equi_Join(f1,f2,idx1,idx2, tr,btree,passed_operator, searchkey = ""):
#	print("non-equi join")
#	b_idx1 = -1
#	b_idx2 = -1
#	for i in range(0,len(btree)):
#		if btree[i].FileName == tr.fName[0] and btree[i].AttributeName == tr.attribute[0]:
#			b_idx1 = i
#		elif btree[i].FileName == tr.fName[1] and btree[i].AttributeName == tr.attribute[1]:
#			b_idx2 = i
#
#	for key in btree[b_idx1].Keys:	
#		for innerkey in btree[b_idx2].Keys:
#			#raw_input(innerkey)
#			if innerkey > key :
#				tr.key.append(key)
#				tr.list1.append(btree[b_idx1].Keys[key])
#				tr.list2.append(btree[b_idx2].Keys[innerkey])
#
#	#Merge the lists created for table 1 and table two
#	tr.merged1 = mergeLists(tr.list1)
#	tr.merged2 = mergeLists(tr.list2)
#	#sort the merged lists
#	tr.merged1.sort()
#	tr.merged2.sort()
#	return tr


