import csv
from sys import getsizeof
import sys
#read each character of the file and append the byte location of each new row to the
#line_offset list
def listRows(fileName,line_offset):
	print("Please be patient... Preprocessing file")
	indexName = fileName.replace(".csv",".index")
	indexFile = open('Indexes/' + indexName, 'rb',0)
	for line in csv.reader(indexFile):
		line_offset.append(int(line[1]))
	return

#an alternative way of getting the line numbers for a file
def createIndex2(fileName,f,line_offset):
	#read each line in file
	print("Please be patient... creating an index")
	maxRows = 0
	#identify the maximum number of rows in the file
	for x in csv.reader(f):
		maxRows +=1
	
	#raw_input("MAX ROWS: " + str(maxRows))
	#return
	offset = 0	
	f.seek(0)
	num = 0
	newLine = True
	indexFile = fileName.replace(".csv",".index")
	out = open("Indexes/" + indexFile,"w")
	sys.stdout = out

	for line in f:
		#if this is a new line, record the location of the previous offset for a new row
		if newLine:
			line_offset.append(offset)
			print str(num) + "," + str(offset)
			num +=1
			if num >maxRows:
				break
		#reset sentinel and coutner before examining line
		charcount = 0
		newLine = False
		#read each character on the line end increment offset
		#if CRLF, set flag
		for c in line:
			if line[charcount] =='\n' :
				newLine = True
			charcount+=1
			offset += 1
			#exit for if flag set
			if newLine:
				break
	out.close()
	sys.stdout = sys.__stdout__
	#go to start of file
	f.seek(0)

#This function creates an index of the start byte for each row in the file
def createIndex(fileName, f,line_offset):
	#read each line in file
	print("Please be patient... creating an index")
	maxRows = 0
	#identify the maximum number of rows in the file
	for x in csv.reader(f):
		maxRows +=1
	
	#raw_input("MAX ROWS: " + str(maxRows))
	#return
	offset = 0	
	f.seek(0)
	#raw_input(f.tell())
	#return
	num = 0
	newLine = True
	indexFile = fileName.replace(".csv",".index")
	out = open("Indexes/" + indexFile,"w")
	sys.stdout = out
	reader = csv.reader(f)
	for line in f:
		#if this is a new line, record the location of the previous offset for a new row
		if newLine:
			line_offset.append(offset)
			print (str(num) + "," + str(offset))
			num +=1
			if num >maxRows:
				#print ("MAX ROWS")
				break
		#reset sentinel and coutner before examining line
		charcount = 0
		newLine = False
		#read each character on the line end increment offset
		#if CRLF, set flag
		for c in line:
			if (line[charcount-1] == '\r' and line[charcount] =='\n'):
				
				step = 2	
				RowEnd =True			
				#if num >4085:
				searchChar = None
				try:
					if num > 1:
						while RowEnd and searchChar !=',':
							searchChar = line[charcount-step]
							if searchChar !=',':
								if not searchChar.isdigit():	
									#raw_input("upper" + str(line[charcount-step]))
								#else:
									RowEnd = False
									#raw_input("lower" + str(line[charcount-step]))
							step+=1
						#if not notNewLine:
							#newLine = True
				except:
					continue
				if RowEnd:				
					#newLine = True
				

					#pos = f.tell()
						#raw_input(f.tell())	
					f.seek(offset+1)
						#raw_input("offset {}".format(f.tell()))
					try:
						f.seek(offset+1)
						val = f.read(1)
							#if num >4085:
							#	raw_input("read {} is int{}".format(val,val.isdigit()))
						f.seek(offset+1)
						if val.isdigit():
							newLine = True
						
					except:
						continue
					f.seek(offset+1)


			charcount+=1
			offset += 1
			#exit for if flag set
			if newLine:
				break
	out.close()
	sys.stdout = sys.__stdout__
	#go to start of file
	f.seek(0)

#This function creates an index of the start byte for each row in the file
#an alternative way of getting the line numbers for a file
def createIndex3(fileName,f,line_offset):
	#read each line in file
	print("Please be patient... creating an index")
	maxRows = 0
	#identify the maximum number of rows in the file
	for x in csv.reader(f):
		maxRows +=1
	
	#raw_input("MAX ROWS: " + str(maxRows))
	#return
	offset = 0	
	f.seek(0)
	#raw_input(f.tell())
	#return
	num = 0
	newLine = True
	indexFile = fileName.replace(".csv",".index")
	out = open("Indexes/" + indexFile,"w")
	sys.stdout = out
	#reader = csv.reader(f)
	for line in f:
		#if this is a new line, record the location of the previous offset for a new row
		if newLine:
			line_offset.append(offset)
			print str(num) + "," + str(offset)
			num +=1
			if num >maxRows:
				#print ("MAX ROWS")
				break
		#reset sentinel and coutner before examining line
		charcount = 0
		newLine = False
		#read each character on the line end increment offset
		#if CRLF, set flag
		for c in line:
			if (line[charcount-1] == '\r' and line[charcount] =='\n'):
				
				step = 2	
				RowEnd =True			

				if RowEnd:				
					#newLine = True
				

					#pos = f.tell()
						#raw_input(f.tell())	
					f.seek(offset+1)
						#raw_input("offset {}".format(f.tell()))
					try:
						f.seek(offset+1)
						val = f.read(1)
							#if num >4085:
							#	raw_input("read {} is int{}".format(val,val.isdigit()))
						f.seek(offset+1)
						if val !='\\':
							newLine = True
						#else:
							#raw_input("ROW {}".format(num))
						
					except:
						continue
					f.seek(offset+1)


			charcount+=1
			offset += 1
			#exit for if flag set
			if newLine:
				break
	out.close()
	sys.stdout = sys.__stdout__
	#go to start of file
	f.seek(0)

#This function replaces non-line terminating line feeds '\n' with " " and renames the file
#as _modified.csv
def cleanCSV(fileName, f,line_offset):

	#read each line in file
	print("Please be patient... cleaning csv file")
	maxRows = 0
	#identify the maximum number of rows in the file
	for x in csv.reader(f):
		maxRows +=1

	offset = 0	
	f.seek(0)
	num = 0
	newLine = True
	indexFile = fileName.replace(".csv","_modified.csv")
	out = open(indexFile,"w")
	sys.stdout = out
	buf = []
	for line in f:
		#if this is a new line, increment row count. Do this until
		#num exceeds the maxRows of the file
		if newLine:
			num +=1
			if num >maxRows:
				break
		#reset sentinel and coutner before examining line
		charcount = 0
		newLine = False
		#read each character on the line and increment offset
		#if CRLF, set flag
		for c in line:
			#replace new line characters with a space if not preceeded
			#by a carriage return, otherwise print the character
			if line[charcount-1] != '\r' and line[charcount] =='\n' :
				buf.append(" ")
			elif line[charcount-1] != '\r' and line[charcount] !='\n' :
				buf.append(c)	
			if line[charcount-1] == '\r' and line[charcount] =='\n' :
				newLine = True
			
			charcount+=1

			#exit for if flag set
			if newLine:
				print ''.join(buf)
				buf = None
				buf = []
				break
	out.close()
	sys.stdout = sys.__stdout__
	#go to start of file
	f.seek(0)
