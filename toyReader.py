import csv
import time
import itertools
from sys import getsizeof
import sys
from enum import IntEnum
from createRowIndex import *
from dataSort import *
from mydebug import *
#from BTrees.OOBTree import *
from btreez_load import *
from btreez import *
#from test import *
import psutil
import numpy as np
from joinFuncs import *
import re
from fileManager import *
from sqlparse_functions import *
from tupleresults import *
#the global table names list
table_names = []
d = {}

class rowIndices():
	def __del__(self):
		print "rowIndices DELETED"
	def __init__(self):
		self.fName = []
		self.line_offset = []
		self.fd = []

class option(IntEnum):
	choose_file = 1	
	show_header =2
	show_row = 3
	show_column_value = 4
	count_rows = 5
	create_index = 6
	clean_csv = 7
	show_range = 8
	create_btree = 9
	display_btree = 10
	create_key_valueIdx = 11
	match_tables_on_join_attribute = 12
	filter_on_where_clause = 13
	exit_loop = 14

#A function that just delcares global variables
def projectGlobals():
	global table_names

	
def Main():
	fileName = None
	f = None
	choice = None
	line_offset = []
	btree = []
	print(psutil.virtual_memory())
	while choice!= option.exit_loop:
		choice = None
		choice = menu()
		print ("")
		if choice == option.choose_file:
			fileName = getFileName()
			f = openCSV(fileName)
			start = time.time()
			line_offset = None
			line_offset = []
			print "CREATING LIST"
			print(psutil.virtual_memory())
			listRows(fileName, line_offset)
			print(psutil.virtual_memory())
			end = time.time()
			print ("Process Time: " + str((end - start)*1000) + "ms")
			#createComparison(line_offset,f)
		if choice ==option.show_header:
			showHeader(f)	
		elif choice ==option.show_row:
			showRow(f,line_offset)
		elif choice == option.show_column_value:
			vals = showColumnValue(f,line_offset)
		elif choice == option.count_rows:
			showCount(line_offset)
		elif choice==option.create_index:
			f = None
			while f==None:
				fileName = getFileName()				
				f = getOpenFileObject(fileName)
				if f!=None:
					line_offset = None
					line_offset = []
					#For photos.csv
					if fileName == 'photos.csv':
						createIndex3(fileName, f,line_offset)
					elif fileName == 'business.csv' or fileName == 'checkin.csv':
					#for business.csv and checkin.csv
						createIndex2(fileName, f,line_offset)
					else:
					#for review.csv
						createIndex(fileName, f,line_offset)
		elif choice==option.clean_csv:
			f = None
			while f==None:
				fileName = getFileName()				
				f = getOpenFileObject(fileName)
				if f!=None:
					line_offset = None
					line_offset = []
					cleanCSV(fileName, f,line_offset)
		elif choice == option.show_range:
			showRange(f,line_offset)
		elif choice == option.create_btree:
			
			#reset btree list
			btree= []
			ri = rowIndices()
			for j in range(0,2):
				if j==0:
					fileName = 'review_5k.csv'
					ri = createRowIndices(ri,fileName)
				elif j ==1:
					fileName = 'photos.csv'
					ri = createRowIndices(ri,fileName)
				elif j ==2:
					fileName = 'business.csv'
					ri = createRowIndices(ri,fileName)
				elif j ==3:
					fileName = 'review_500k.csv'
					ri = createRowIndices(ri,fileName)
				elif j ==4:
					fileName = 'review_50k.csv'
					ri = createRowIndices(ri,fileName)
				elif j ==5:
					fileName = 'checkin.csv'
					ri = createRowIndices(ri,fileName)
				elif j ==6:
					fileName = 'review_1m.csv'
					ri = createRowIndices(ri,fileName)
				f = openCSV(fileName)		
				cols = getNumberOfColumns(f)
				for i in range(0,cols):
					attributeName = getAttributeName(f,i)
					#raw_input(attributeName)
					#if attributeName  !='text' and attributeName  !='user_id':
					d = getAllColumnValues2(f,line_offset,i)
					#s = collections.OrderedDict(sorted(d.items()))
					#if fileName != 'review_1m.csv' and fileName != 'review_500k.csv':
					try:
						createDiskTree(d,fileName,attributeName)
					except:
						pass
					#else:
					#btree.append(createMemoryTree(d,fileName,attributeName))
					d.clear()
					#s.clear()
			#BTreeTest(d)	
			#sortData2(d, fileName)
			#getValues(d)
			
					print(psutil.virtual_memory())

		elif choice == option.display_btree:
			fileName = getFileName()			
			f = getOpenFileObject(fileName)
			idx = enterAttributeIndex(f)
			attributeName = getAttributeName(f,idx)
			key = getKey()
			for i in btree:
				if i.FileName == fileName and i.AttributeName == attributeName:
					raw_input("FileName: {0}\nAttribute: {1}\nKey: {2}\n"\
					"Values: {3}".format(fileName,attributeName,key,i.Keys[key]))
		#loadTree(fileName,attributeName)
		elif choice == option.create_key_valueIdx:
			fileName = getFileName()
			f = getOpenFileObject(fileName)
			line_offset = []
			listRows(fileName, line_offset)
			cols = getNumberOfColumns(f)
			for i in range(0,cols):
				attributeName = getAttributeName(f,i)
				d = getAllColumnValues2(f,line_offset,i)
				sortData2(d, fileName,attributeName)
				d.clear()
		elif choice == option.match_tables_on_join_attribute:	
			if len(btree):
				test_conditions=[5]
				start = time.time()
				if 0 in test_conditions:
					sql = raw_input("Enter query")
					start = time.time()
					tr = []
					tr.append(tupleResults())
					tr.append(tupleResults())
					#try:					
					runQuery(tr,btree,ri,sql,start)
					#except Exception as e:
					#	print ("bad query " + str(e))
					continue
				if 1 in test_conditions:				
					#############################
					#TEST 1
					#############################
					tr = []
					tr.append(tupleResults())
					single_table_invalid_key(tr,btree)
					#continue
				elif 2 in test_conditions:
					#############################
					#TEST 2
					#############################
					tr = []
					tr.append(tupleResults())
					single_table_valid_key(tr,btree)
					#continue
				elif 3 in test_conditions:
					#############################
					#TEST 3
					#############################
					tr = []
					tr.append(tupleResults())
					exampleQuery4(tr,btree)
					
				elif 4 in test_conditions:
					#############################
					#TEST 4
					#############################
					tr = []
					tr.append(tupleResults())
					tr.append(tupleResults())
					singleJoin(tr,btree,ri)
					end = time.time()
					raw_input("This join operation took {} seconds".format(end - start))
					#continue
				elif 5 in test_conditions:
					#############################
					#TEST 5
					#############################
					tr = []
					tr.append(tupleResults())
					tr.append(tupleResults())
					doubleJoin(tr,btree,ri)
					end = time.time()
					raw_input("This join operation took {} seconds".format(end - start))
					#continue
				elif 6 in test_conditions:

					#############################
					#TEST 6
					#############################
					tr = []
					tr.append(tupleResults())
					tr.append(tupleResults())
					exampleQuery5(tr,btree,ri)			
					#end1 = time.time()
				elif 7 in test_conditions:
					#############################
					#TEST 7
					#############################
					tr = []
					tr.append(tupleResults())
					tr.append(tupleResults())
					exampleQuery3(tr,btree,ri)			
					end = time.time()
					raw_input("This join operation took {} seconds".format(end - start))
				elif 8 in test_conditions:
					#############################
					#TEST 8
					#############################
					tr = []
					tr.append(tupleResults())
					tr.append(tupleResults())
					selfJoin(tr,btree,ri)
					end = time.time()
					raw_input("This join operation took {} seconds".format(end - start))
					#continue
					#reduced1 = reducedList(tr1.merged2,tr2.merged2)
					#filterOnKeys(tr1,reduced1)
				
					#print reduced1					
					#end2 = time.time()
					#raw_input("This join operation took {} seconds {}".format(end1 - start,end2 - start))
			else:
				print ("btree must be created before it can be displayed")
		elif choice == option.filter_on_where_clause:
			#sql_str = "SELECT R.business_id FROM review_5k.csv R WHERE (R.business_id = 'A6lKCuTrDSJ_eFKyumZCJQ' OR R.business_id = 'uYHaNptLzDLoV_JZ_MuzUA');"
			#sql_str = "SELECT R.business_id, P.photo_id FROM review_5k.csv R JOIN photos.csv P ON R.business_id = P.business_id WHERE R.stars = 5;"
			sql_str = raw_input('Enter SQL> ')
			#showTree(sqlparse.parse(sql_str)[0])
			start = time.time()
			parse_result = sqlparse.parse(sql_str)
			atDict = getTablesAndAliases(parse_result)
			indexTR = tupleResults()
			getIndexNamesToLoad(parse_result[0], indexTR, atDict, from_seen = False)
			indicesLoaded = {}
			for i in range(0,len(indexTR.fName)):
				fName = indexTR.fName[i]
				attrib = indexTR.attribute[i]
				iKey = fName + '>' + attrib
				if iKey in indicesLoaded:
					continue
				else:
					indicesLoaded[iKey] = True
				bt = loadTree(fName,attrib)
				tg = treeGroup(fName,attrib,bt)
				btree.append(tg)
			showTreeGroups(btree)
			uniquefiles = {}
			for k in atDict:
				uniquefiles[atDict[k]] = k
			ri = rowIndices()
			for fn in uniquefiles:
				ri = createRowIndices(ri, fn)

			resultdict = whereClauseEval(parse_result, btree)
			#print resultdict
			onTRList = extractOnConditionsFromParsed(parse_result)


			
			
			#print onTRList[0].alias
			#print onTRList[0].attribute
			#tr_from_where = tupleResults()
			#for k in resultdict:
			#	tr_from_where.alias.append(k)
			#	tr_from_where.fName.append(atDict[k])
			

			myTuples = []

			if len(atDict) == 1:
				for k in atDict: #just getting at the single key
					myTuples = []
					for i in resultdict[k]:
						myTuples.append([i])

					mtr = tupleResults()
					mtr.alias.append(k)
					mtr.fName.append(atDict[k])

					mergedTR = [mtr]
			elif len(atDict) == 2:
				mergedTR = [equalityJoin(None,None,onTRList[0],btree)]
				for tr in mergedTR:
					#print tr.alias
					for i in range(0,len(tr.alias)):
						if tr.alias[i] in resultdict:
							for j in range(0,len(tr.list[i])):
								tr.list[i][j] = list(resultdict[tr.alias[i]].intersection(set(tr.list[i][j])))
				myProduct = []
				for i in range(0,len(mergedTR[0].list[0])):
					a = mergedTR[0].list[0][i]
					b = mergedTR[0].list[1][i]
					#raw_input("b is {}".format(b))
					myProduct.append(product(a,b))
				myTuples = []
				#print myProduct
				for i in myProduct:
					for j in i:
						myTuples.append(j)
						#print j
				#displayTuples(mergedTR[0], ri)
			elif len(atDict) == 3:
				for i in range(0,len(onTRList)):
					onTRList[i] = equalityJoin(None,None,onTRList[i],btree)
					print 'equalityJoin #' + str(i)
				for tr in onTRList:
						for i in range(0,len(tr.alias)):
							if tr.alias[i] in resultdict:
								for j in range(0,len(tr.list[i])):
									tr.list[i][j] = list(resultdict[tr.alias[i]].intersection(set(tr.list[i][j])))
				ur = {}
				ur = populateUltimateResults(ur,onTRList,atDict)
				print 'ur populated'
				myProduct = displayTuples(onTRList,ri,ur,atDict)
				myTuples = []
				for i in myProduct:
					for j in i:
						#print j
						myTuples.append(j)
				mergedTR = onTRList
				joinTR = tupleResults()
				seen_alias = {}
				for i in range(0, len(mergedTR)):
					for j in range(0, len(mergedTR[i].alias)):
						alias = mergedTR[i].alias[j]
						if alias in seen_alias:
							continue
						seen_alias[alias] = True
						joinTR.alias.append(alias)
						joinTR.fName.append(mergedTR[i].fName[j])
				print joinTR.alias
				print joinTR.fName
			#print output_rows[:10]
			#for r in output_rows:
			#	raw_input(r)
			if len(atDict) == 3:
				output_rows = postJoinFiltering(parse_result, myTuples, joinTR, ri)
				final_output = applyProjection(output_rows, joinTR, parse_result)
			else:
				output_rows = postJoinFiltering(parse_result, myTuples, mergedTR[0], ri)
				final_output = applyProjection(output_rows, mergedTR[0], parse_result)
			end = time.time()
			print final_output[:50]
			print 'First 50 rows shown, ' + str(len(final_output)-1) + ' rows in total'
			print "Process Time: " + str((end - start)*1000) + "ms"
		
	print ("Bye")
	return
def createRowIndices(ri,fileName):
	ri.fName.append(fileName)
	ri.fd.append(getOpenFileObject(fileName))
	idx = len(ri.fName)-1
	ri.line_offset.append([])
	listRows(fileName, ri.line_offset[idx])
	#createIndex(ri.fName[idx], ri.fd[idx],ri.line_offset[idx])
	return ri

def getFileName():
	return raw_input("Type a file name, or type exit >>>")
def getKey():
	return raw_input("Enter a key >>>")
def enterAttributeIndex(f):
	return int(raw_input("Enter an index for the attribute between 0 and {}, or type exit >>>".format(getNumberOfColumns(f)-1)))

def showHeader(f):
	reader = csv.reader(f)
	f.seek(0)
	print (reader.next())
	f.seek(0)

def showRow(f,line_offset):
	row = 0
	try:
		while row ==0:	
			row = int(raw_input("Enter a row number>>>"))
			f.seek(0)
			reader = csv.reader(f)
			N = line_offset[row]
			print ("characters " + str(N))

			f.seek(N)
			print ("went to row {} ".format(row))
			print (reader.next())
	except:
		print ("error")
		showRow(f,line_offset)

#demonstrates loading a range into memory and printing out the results
def showRange(f,line_offset):
	try:
		startRow = getStartRow()
		endRow = getEndRow()
		start = time.time()
		f.seek(0)
		reader = csv.reader(f)		
		results = []
		for i in range (startRow,endRow+1):			
			N = line_offset[i]
			f.seek(N)
			record = reader.next()
			results.append(N)

		end = time.time()
		print ("It took {0} ms to print {1} rows".format(end-start,len(results)))
		decision = ""
		
		decision = raw_input("Enter y to print results? ")
		if decision == "y":
			counter = 1
			for i in results:
				f.seek(i)
				output = reader.next()
				print ("\n{0}	{1}".format(counter, output))
				counter+=1
	except:
		print ("error")
		showRow(f,line_offset)

def getStartRow():
	startRow = -1
	while startRow ==-1:	
		startRow = int(raw_input("Enter the start row number>>>"))
	return startRow
def getEndRow():
	endRow = -1
	while endRow ==-1:	
		endRow = int(raw_input("Enter the end row number>>>"))
	return endRow

def getCols():
	cols = []
	inputstring = ""
	while inputstring != "exit":
		try:
			inputstring = raw_input("enter a column number. enter exit to end")
			cols.append(int(inputstring))
		except:
			if (inputstring != "exit"):
				print ("value must be numeric")
				inputstring = ""
	return cols

def showColumnValue(f,line_offset):
	f.seek(0)
	reader = csv.reader(f)		
	startRow = getStartRow()
	endRow = getEndRow()
	included_cols = getCols()
	results = []
	for i in range (startRow,endRow+1):			
		N = line_offset[i]
		f.seek(N)
		cols =reader.next()
		content = list(cols[i] for i in included_cols)
		print (content)
		results.append(content)
	return results

def getAllColumnValues2(f,line_offset,col):
	f.seek(0)
	reader = csv.reader(f)		

	#skip the header row
	reader.next()
	results = []
	count = 1
	d = {}
	keysize1 = 0
	keysize2 = 0
	for row in reader:
		key =  row[col]
		if key in d:
			#if this string is 0-9 from beginning to end, then match, else None
			#and re.match(r'^\d+',key)!=None
			if key.isdigit():
				d[int(key)].append(count)
			else:				
				d[key].append(count)
		else:
			if key.isdigit() :
				d.setdefault(int(key),[])
				d[int(key)].append(count)
			else:
				d.setdefault(key,[])
				d[key].append(count)
		count+=1	
	
	return d

def getAllColumnValues(f,line_offset):
	
	f.seek(0)
	reader = csv.reader(f)		
	included_cols = getCols()
	#skip the header row
	reader.next()
	results = []
	count = 1
	for row in reader:
		content = list(row[j] for j in included_cols)
		results.append(content)

	return results

def showCount(line_offset):
	raw_input("# elements indexed: {0}\n#data records {1}".format(len(line_offset),len(line_offset)-1))

def menu():
	selection = 0
	while selection < option.choose_file or selection > option.exit_loop:
		try:
			selectionString = "Choose an option:\n"\
				"\t{0}.	Choose a file\n"\
				"\t{1}.	Show header\n"\
				"\t{2}.	Show row\n"\
				"\t{3}.	Show value from a given column in a row\n"\
				"\t{4}.	Show count of total data rows\n"\
				"\t{5}.	Create index for file\n"\
				"\t{6}.	Clean CSV File\n"\
				"\t{7}.	Show Range of Records\n"\
				"\t{8}.	Create B+ Tree\n"\
				"\t{9}.	Display B+ Tree\n"\
				"\t{10}.	Create Key-Value Lookup\n"\
				"\t{11}.	Match tables on join attribute\n"\
				"\t{12}.	SQL Query\n"\
				"\t{13}.	Exit\n".format(
					option.choose_file,
					option.show_header,
					option.show_row,
					option.show_column_value,
					option.count_rows,
					option.create_index,
					option.clean_csv,
					option.show_range,
					option.create_btree,
					option.display_btree,
					option.create_key_valueIdx,
					option.match_tables_on_join_attribute,
					option.filter_on_where_clause,
					option.exit_loop)
			
			selection = int(raw_input(selectionString))

			if (selection < option.choose_file) or (selection > option.exit_loop):
				print ("valid choices are between 1 and 5")
			else:
				return selection
		except:
			print ("invalid entry")
			selection = 0
	#return menu()


def countRows():
	print("# records: " + str(len(line_offset)))
#add all files in directory to global dataframe object. 
#Make sure to name each dataframe
def buildDataFrames():
	listOfFiles = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith(".csv")]
	for index, g in enumerate(listOfFiles[0:len(listOfFiles)]):
		table_names.insert(len(table_names),g)

Main()

