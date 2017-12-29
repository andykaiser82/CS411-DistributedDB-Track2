import csv
from fileManager import *
from createRowIndex import *
from joinFuncs import *
from btreez_load import *
from btreez import *
from sqlparse_functions import *

def populateUltimateResults(ur,tr,at_dict):

	#iterate through each tupleResult and add unique file names (aliases)
	#to the dict's set of keys with None as the default value
	for i in tr:
		for name in i.alias:
			if name !='':
				#default the ur for each key to an empy set
				if name not in ur:
					ur[name] = []
	#iterate through each key in the dict
	#for key in ur:	
	#Compare the alias name to the key of the dict
	for i in range(0,len(tr)):
		#get each alias for the tr	
		for j in range(0,len(tr[i].alias)):
			key = tr[i].alias[j]
			#raw_input("key {}".format(key))
			#add the merged set of rows to the key of the tr 
			if ur[key] == []:
				ur[key] = (tr[i].merged[j])
				#raw_input("{} added for key {}".format(ur[key],key))
			else:
				#if an alias was involved in a previous JOIN
				#then an intersection should to be applied to it
				currentList = []
				additionalList = []
				currentList = ur[key]
				additionalList = tr[i].merged[j]
				intersected_list = findIntersection(currentList,additionalList)
				ur[key] = intersected_list
				tr[i].merged[j] =intersected_list
				#raw_input("{} updated for key".format(ur[key],key))

	return ur

def displayTuples(tr,ri,ur,at_dict):
	myProduct = []
	foundAlias = {}
	testProduct = []
	
	tupleCount = 0
	smallProduct = []
	
	for i in range(0,len(tr)):
		if i==0:
			if not tr[i].alias[0]  in foundAlias:
				#"ADDING KEY TO dictionary to associate an aliases order of appearance with the alias name
				foundAlias[tr[i].alias[0]] = len(foundAlias)
			if not tr[i].alias[1]  in foundAlias:
				foundAlias[tr[i].alias[1]]= len(foundAlias)
			#Creates a cartesian product for each list in the the tr 
			for j in range(0,len(tr[i].list[0])):
				a = tr[i].list[0][j]
				b = tr[i].list[1][j]
				#raw_input("b is {}".format(b))
				myProduct.append(product(a,b))
		else:
			dummyProduct = []
			for p in myProduct:
				for q in p:
					dummyProduct.append(q)
			for j in range(0,2):
				smallProduct = []
				thisalias = tr[i].alias[j]
				thislist = tr[i].list[j]
				#only want to perform the following for the new alias
				if not thisalias  in foundAlias:
					#add the alias to the list of all aliases
					foundAlias[thisalias]= len(foundAlias)
					#Need to only join c on a x b for 
					# a = c if a was involved in a x b
					for k in range(0,len(thislist)):
						#get the list of rows for the new table
						#that matches on a key
						c = thislist[k]
						#get the index of the other table
						#that was involved in a previous join
						if j == 0:
							m = 1
						else:
							m = 0
						#get the list of rows from the other table
						d = tr[i].list[m][k]
						#get the alias name of other table involved in this ON clause
						alias = tr[i].alias[m]
						#for the other table involved in this ON clause 
						#compare the previous list of rows that are valid with the 
						#current set of rows that are valid
						previousList =list(ur[alias])
						currentList = list(d)
						#update the merged list for the table involved in a previous
						#on clause
						intersected_list = list(findIntersection(previousList,currentList))
					
						#get the position of the element in the previous
						#cross product (i.e., did that table name appear first or second?)
						#that corresponds to this JOIN ON's table from a previous JOIN ON clause
						pos = foundAlias[alias]

						#if the row was found in the merged list for the alias, 
						#continue evaluation 
						
						if len(intersected_list) > 0:
							#Check each previous cross product
							p_counter = 0
							for q in dummyProduct:
								analyzer = 0
								#check each value of the table	
								p_counter +=1	
								#check if the value in question is valid
								if q[pos] in intersected_list:

									if analyzer == 0:
										analyzer+=1
									doublet = tuple()
									#Create the doublet for the new product
									for r in q:		
										doublet = doublet + (r,)

										#create the cross product: cannot use
										#itertools because the cross product that
										#is needed is in the form of (A,B,C) 
										#append each row in c to each product in myProduct
									for row in c:
										triplet = tuple()			
										triplet = doublet + (row,)
										smallProduct.append(triplet)
									
				if smallProduct != []:
					testProduct.append(smallProduct)	
	
	if len(smallProduct) == 0:
		#print("Counting myProduct")
		#count= countTuples(myProduct)
		#print("{}\nCartesian product of rows: {}".format(list(myProduct),count))
		#printTuples(myProduct,ri,tr,foundAlias,at_dict)
		return myProduct
	else:
		#print("Counting testProduct")
		#count= countTuples(testProduct)
		#print("{}\nCartesian product of rows: {}".format(list(testProduct),count))
		#printTuples(testProduct,ri,tr,foundAlias,at_dict)
		return testProduct

def printTuples(myProduct,ri,tr,foundAlias,at_dict):
	count = 0
	idx_alias_dict = {}
	
	for key in foundAlias:
		idx_alias_dict[foundAlias[key]] = key

	#iterate over each set of products
	for i in myProduct:
		#iterate over each cross product in a set
		for j in i:
			rowResult = [] 
			#iterate over each row listed in the cross product
			for k in range(0,len(j)):
				#find the file name of the rowIndex member that 
				#matches the filename of the source row
				alias = idx_alias_dict[k]
				fName = at_dict[alias]
				for l in range(0,len(ri.fName)):
					#navigate to the row in question and 
					#print the result
					if fName == ri.fName[l]:
						row = j[k]
						N = ri.line_offset[l][row]
						ri.fd[l].seek(N)
						reader = csv.reader(ri.fd[l])
						rowResult +=  reader.next()

			#raw_input(rowResult)
				
def countTuples(result):
	count = 0
	for i in result:
		for j in i:
			#print j
			count+=1
	print count
	return count
		
def displayRowsOfTable(ur):
	for key in ur:
		rows = ur[key]
		print("Number of values: {} \nkey: {} ".format(len(rows),key))

def runQuery(tr,btree,ri,sql,start):
	#print ("************************************************\n"\
	#"TEST Simple join on with one attribute\n"\
	#"EXAMPLE 'Select * from review_5k.csv r JOIN photos.csv p "\
	#"ON r.business_id = p.business_id JOIN review_5k.csv rv ON r.business_id = rv.business_id'\n"\
	#"************************************************\n")
	#testFile1 = 'review_5k.csv'	
	#testFile2 = 'photos.csv'
	#sql = 'Select * from review_5k.csv r JOIN photos.csv p '\
	#'ON r.business_id = p.business_id JOIN review_5k.csv rv ON r.business_id = rv.business_id'
	at_dict = getAliasDictFromSQL(sql)
	tr = extractOnConditions(sql, 0)
	#for i in range(0,len(tr)):
	#	for j in range(0,2):
	#		raw_input("tr[{0}].fName[{1}]: {2}\ntr[{0}].alias[{1}]: {3}"\
	#		.format(i,j,tr[i].fName[j],tr[i].alias[j]))

	for i in range(0,len(tr)):
		f1 = getOpenFileObject(tr[i].fName[0])
		cols1 = getNumberOfColumns(f1)
		f2 = getOpenFileObject(tr[i].fName[1])
		start = time.time()	
		tr[i] = equalityJoin(f1,f2,tr[i],btree)

	#for i in range(0,len(tr)):
	#	for j in range(0,2):
	#		raw_input("{4}\ntr[{0}].list[{1}]\ntr[{0}].fName[{1}]: {2}\ntr[{0}].alias[{1}]: {3}"\
	#		.format(i,j,tr[i].fName[j],tr[i].alias[j],tr[i].list[j]))
			
	#ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr,at_dict)
	end = time.time()
	displayRowsOfTable(ur)
	displayTuples(tr,ri,ur,at_dict)
	raw_input("This query took {} seconds".format(end - start))
	#return tr

#Simple join on with one attribute
def singleJoin(tr,btree,ri):
	print ("************************************************\n"\
	"TEST Simple join on with one attribute\n"\
	"EXAMPLE 'Select * from review_5k.csv r JOIN photos.csv p "\
	"ON r.business_id = p.business_id'\n"\
	"************************************************\n")
	#testFile1 = 'review_5k.csv'	
	#testFile2 = 'photos.csv'
	sql = 'SELECT * FROM review_5k.csv r JOIN photos.csv p '\
	'ON r.business_id = p.business_id'
	at_dict = getAliasDictFromSQL(sql)
	tr =extractOnConditions(sql, 0)

	for i in range(0,len(tr)):
		for j in range(0,2):
			raw_input("tr[{0}].fName[{1}]: {2}\ntr[{0}].alias[{1}]: {3}"\
			.format(i,j,tr[i].fName[j],tr[i].alias[j]))

	for i in range(0,len(tr)):
		f1 = getOpenFileObject(tr[i].fName[0])
		cols1 = getNumberOfColumns(f1)
		f2 = getOpenFileObject(tr[i].fName[1])
		start = time.time()	
		tr[i] = equalityJoin(f1,f2,tr[i],btree)
	#for i in range(0,len(tr)):
	#	raw_input("TR num {}".format(i))
	#return
	#ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr,at_dict)
	displayRowsOfTable(ur)
	raw_input("Results should be 137119")
	raw_input("Display Tuples")
	displayTuples(tr,ri,ur,at_dict)

#Simple join on with one attribute
def selfJoin(tr,btree,ri):
	print ("************************************************\n"\
	"TEST Simple join on with one attribute\n"\
	"EXAMPLE 'Select * from review_5k.csv r JOIN  review_5k.csv rv "\
	"ON r.business_id = rv.business_id'\n"\
	"************************************************\n")
	#testFile1 = 'review_5k.csv'	
	#testFile2 = 'photos.csv'
	sql = 'SELECT * FROM review_5k.csv r JOIN review_5k.csv rv '\
	'ON r.business_id = rv.business_id'
	at_dict = getAliasDictFromSQL(sql)
	tr =extractOnConditions(sql, 0)

	for i in range(0,len(tr)):
		for j in range(0,2):
			raw_input("tr[{0}].fName[{1}]: {2}\ntr[{0}].alias[{1}]: {3}"\
			.format(i,j,tr[i].fName[j],tr[i].alias[j]))

	for i in range(0,len(tr)):
		f1 = getOpenFileObject(tr[i].fName[0])
		cols1 = getNumberOfColumns(f1)
		f2 = getOpenFileObject(tr[i].fName[1])
		start = time.time()	
		tr[i] = equalityJoin(f1,f2,tr[i],btree)
	#for i in range(0,len(tr)):
	#	raw_input("TR num {}".format(i))
	#return
	#ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr,at_dict)
	displayRowsOfTable(ur)
	raw_input("Results should be 5002")
	raw_input("Display Tuples")
	displayTuples(tr,ri,ur,at_dict)

def doubleJoin(tr,btree,ri):
	print ("************************************************\n"\
	"TEST Simple join on with one attribute\n"\
	"EXAMPLE 'Select * from review_5k.csv r JOIN photos.csv p "\
	"ON r.business_id = p.business_id JOIN review_5k.csv rv ON r.business_id = rv.business_id'\n"\
	"************************************************\n")
	#testFile1 = 'review_5k.csv'	
	#testFile2 = 'photos.csv'
	sql = 'Select * from review_5k.csv r JOIN photos.csv p '\
	'ON r.business_id = p.business_id JOIN review_5k.csv rv ON r.business_id = rv.business_id'
	at_dict = getAliasDictFromSQL(sql)
	tr = extractOnConditions(sql, 0)
	for i in range(0,len(tr)):
		for j in range(0,2):
			raw_input("tr[{0}].fName[{1}]: {2}\ntr[{0}].alias[{1}]: {3}"\
			.format(i,j,tr[i].fName[j],tr[i].alias[j]))

	for i in range(0,len(tr)):
		f1 = getOpenFileObject(tr[i].fName[0])
		cols1 = getNumberOfColumns(f1)
		f2 = getOpenFileObject(tr[i].fName[1])
		start = time.time()	
		tr[i] = equalityJoin(f1,f2,tr[i],btree)

	for i in range(0,len(tr)):
		for j in range(0,2):
			raw_input("{4}\ntr[{0}].list[{1}]\ntr[{0}].fName[{1}]: {2}\ntr[{0}].alias[{1}]: {3}"\
			.format(i,j,tr[i].fName[j],tr[i].alias[j],tr[i].list[j]))
			
	#ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr,at_dict)
	displayRowsOfTable(ur)
	#raw_input("Results should be 314 and 2933")
	#raw_input("Display Tuples")
	displayTuples(tr,ri,ur,at_dict)

def exampleQuery3(tr,btree,ri):
	print ("************************************************\n"\
	"TEST Join on one attribute with equality condition"\
	"EXAMPLE 'Select * from review_5k.csv r JOIN business.csv b "\
	"ON r.business_id = p.business_id WHERE b.city = 'Champaign''\n"\
	"************************************************\n")
	testFile1 = 'review_5k.csv'	
	testFile2 = 'business.csv'
	print ("************************************************\n"\
	"TEST Simple join on with one attribute\n"\
	"EXAMPLE 'Select * from review_5k.csv r JOIN photos.csv p "\
	"ON r.business_id = p.business_id'\n"\
	"************************************************\n")
	#testFile1 = 'review_5k.csv'	
	#testFile2 = 'photos.csv'
	sql = 'SELECT * FROM review_5k.csv r JOIN business.csv b '\
	'ON r.business_id = b.business_id'
	at_dict = getAliasDictFromSQL(sql)
	tr =extractOnConditions(sql, 0)
	
	f1 = getOpenFileObject(tr[0].fName[0])
	cols1 = getNumberOfColumns(f1)
	f2 = getOpenFileObject(tr[0].fName[1])
	start = time.time()	
	tr[0] = equalityJoin(f1,f2,tr[0],btree)
	ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr)
	displayRowsOfTable(ur)
	raw_input("Results should be 111")
	raw_input("Display Tuples")
	displayTuples(tr[0],ri)
	#sprint("Results should be 111")
#Single table query with a key that does not exist
def single_table_invalid_key(tr,btree):
	print ("************************************************\n"\
	"TEST Single table query with a key that does not exist\n"\
	"EXAMPLE 'Select * from review_5k.csv Where stars = 72'\n"\
	"************************************************\n")
	testFile1 = 'review_5k.csv'
	sql = 'Select * from review_5k.csv Where stars = 72'
	at_dict = getAliasDictFromSQL(sql)
	tr =extractOnConditions(sql, 0)

	for i in tr:
		for j in range(0,len(i.alias)):
			raw_input("alias {}".format(i.alias[j]))
	#tr[0].fName.append(testFile1)
	#tr[0].attribute.append('stars')	
	f1 = getOpenFileObject(tr[0].fName[0])
	#line_offset1 = []
	#listRows(tr[0].fName[0], line_offset1)
	searchkey = 72
	tr[0] = equalCondition(tr[0],btree,searchkey)

	ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr)
	displayRowsOfTable(ur)
	print("Results should be an empty set")
	filterOnKeys(tr,ur)
#Single table query with a key that exist
def single_table_valid_key(tr,btree):
	print ("************************************************\n"\
	"TEST Single table query with a key that exist\n"\
	"EXAMPLE 'Select * from review_5k.csv Where stars = 3'\n"\
	"************************************************\n")
	testFile1 = 'review_5k.csv'
	tr[0].fName.append(testFile1)
	tr[0].attribute.append('stars')	
	f1 = getOpenFileObject(tr[0].fName[0])
	line_offset1 = []
	listRows(tr[0].fName[0], line_offset1)
	searchkey = 3
	tr[0] = equalCondition(tr[0],btree,searchkey)

	ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr)
	displayRowsOfTable(ur)
	print("Results should be 765")
	filterOnKeys(tr,ur)
#Single table query with a different key that exists
def exampleQuery4(tr,btree):
	print ("************************************************\n"\
	"TEST Single table query with a different key that exists\n"
	"EXAMPLE 'Select * from review_5k.csv Where stars = 4'\n"\
	"************************************************\n")
	testFile1 = 'review_5k.csv'
	tr[0].fName.append(testFile1)
	tr[0].attribute.append('stars')	
	f1 = getOpenFileObject(tr[0].fName[0])
	line_offset1 = []
	listRows(tr[0].fName[0], line_offset1)
	searchkey = 4
	tr[0] = equalCondition(tr[0],btree,searchkey)

	ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr)
	displayRowsOfTable(ur)
	print("Results should be 1247")




def exampleQuery5(tr,btree,ri):
	print ("************************************************\n"\
	"TEST BASELINE: Join on one attribute without the equality condition\n"\
	"************************************************\n")
	testFile1 = 'review_5k.csv'	
	testFile2 = 'business.csv'

	tr[0].fName.append(testFile1)
	#tr.fName1 = getFileName()
	f1 = getOpenFileObject(tr[0].fName[0])
	line_offset1 = []
	listRows(tr[0].fName[0], line_offset1)
	cols1 = getNumberOfColumns(f1)
	#tr.fName2 = getFileName()
	tr[0].fName.append(testFile2)
	f2 = getOpenFileObject(tr[0].fName[1])
	line_offset2 = []
	listRows(tr[0].fName[1], line_offset2)
	cols2 = getNumberOfColumns(f2)
	start = time.time()
	#idx1 = getIndexFromSelection(f1)
	tr[0].attribute.append('business_id')
	#tr1.attribute1 = getAttributeName(f1,idx1)	
	tr[0].attribute.append('business_id')	
	#tr.attribute2 = getAttributeName(f2,idx2)
	#return
	#getAttributeIndex(f1,attributeName)		
	tr[0] = equalityJoin(f1,f2,tr[0],btree)

	ur = ultimateResults()
	ur = {}
	ur = populateUltimateResults(ur,tr)
	displayRowsOfTable(ur)
	print("Results should be 111 and 5002")


