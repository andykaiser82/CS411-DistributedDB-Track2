class tupleResults():
	def __del__(self):
		print "tupleResults DELETED"
	def __init__(self):
		self.key = []
		self.list = []
		self.fName = []#need 2 for joins 
		self.alias = []#need 2 for joins
		self.attribute = []#need 2 for joins
		self.merged = []
		self.operator = ''
