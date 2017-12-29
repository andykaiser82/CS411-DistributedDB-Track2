import csv
def getNumberOfColumns(f):
	f.seek(0)
	reader = csv.reader(f)
	cols = reader.next()
	f.seek(0)
	return len(cols)

def getAttributeIndex(f,attributeName):
	f.seek(0)
	reader = csv.reader(f)
	cols = reader.next()
	f.seek(0)
	for idx in cols:
		if cols[idx] == attributeName:
			return cols[idx]

def getAttributeName(f,idx):
	f.seek(0)
	reader = csv.reader(f)
	attribute = reader.next()
	f.seek(0)
	return attribute[idx]

def getIndexFromSelection(f):
	f.seek(0)
	reader = csv.reader(f)
	cols = reader.next()
	print("Choose one of the available attributes")
	for i in range(0,getNumberOfColumns(f)-1):
		print ("{}\t{}".format(i+1,cols[i]))
	return int(raw_input("Enter an index for the attribute between 1 and {}, or type exit >>>".format(getNumberOfColumns(f)-1)))-1

def getIndexFromAttributeName(f,attributeName):
	f.seek(0)
	reader = csv.reader(f)
	cols = reader.next()
	for i in range(0,getNumberOfColumns(f)-1):
		if cols[i] == attributeName:
			return i

def getOpenFileObject(fileName):
	try:
		f = openCSV(fileName)
	except:	
		if fileName != "exit":
			print ("{} does not exist.".format(fileName))
			return None
	return f

def openCSV(fileName):
	try:
		return open('source_files/' + fileName, 'rb',0)
	except:
		raise "Error"
