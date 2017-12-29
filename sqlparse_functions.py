import sqlparse
from sqlparse.tokens import *
import re
import csv
from tupleresults import *

class CompBlock():
	def __init__(self):
		self.alias = ''
		self.attribute = ''
		self.value = None
		self.operator = ''

def Main():

	#sql = 'SELECT M.director_name FROM movies.csv M, oscars.csv O WHERE M.movie_title = O.Film AND O.Award = "Cinematography" AND O.Winner = True;'
	sql = "SELECT * FROM movies.csv M JOIN oscars.csv A ON M.movie_title = A.Film JOIN business.csv B ON M.rating = B.stars AND M.sales < B.networth WHERE A.Winner = 1 AND (M.imdb_score < 6 OR M.movie_facebook_likes < 10000) AND M.title LIKE 'Face'"
	
	#sql = "SELECT M.movie_title, M.title_year, M.imdb_score, A1.Name, A1.Award, A2.Name, A2.Award FROM business.csv B, movies.csv M JOIN oscars.csv A1 ON (M.movie_title = A1.Film) JOIN oscars.csv A2 ON (M.movie_title = A2.Film) WHERE A1.Award = 'Actor'"
	#sql = "SELECT A.blah, B.hah, C."
	#sql_str = "SELECT R.business_id FROM review_5k.csv R WHERE (R.business_id = 'A6lKCuTrDSJ_eFKyumZCJQ') AND R.stars > 3 AND R.stars < 5 AND R.funny = 0;"
	result = sqlparse.parse(sql)

	#print result[0].tokens
	#join_tokenlist = joinClauseEval(result)

	#print result[0].tokens[-1].tokens
	showTree(result[0])
	#TRList = extractOnConditions(sql, inequalities = True)
	#print len(TRList)
	#for i in TRList:
	#	print 'Join these:'
	#	for j in [0,1]:
	#		print 'set #' + str(j)
	#		print i.alias[j]
	#		print i.fName[j]
	#		print i.attribute[j]

def showTree(tok_list, depth=0):
	for tok in tok_list.tokens:
		if isinstance(tok, sqlparse.sql.TokenList):
			print '  '*depth + str(type(tok)) + '['
			showTree(tok, depth+1)
			print '  '*depth + ']'
		elif isinstance(tok, sqlparse.sql.Token):
			#if tok.ttype == Token.Text.Whitespace:
			#	continue
			print '  '*depth + str(tok.ttype) + ': ' + str(tok.value)
		else:
			raise Error

def whereClauseEval(parseResult, TGList):
	#result = sqlparse.parse(sql)
	at_dict = getTablesAndAliases(parseResult)
	subclauses = whereClauseSplit(parseResult[0].tokens[-1])
	result_list = []
	for i, s in enumerate(subclauses):
		result_list.append(whereSubClauseEval(s,at_dict, TGList))
	return combineAndList(result_list)



#warning: wraps subclauses in tokenList even if subclause is a single tokenList
def whereClauseSplit(whereTokenList):
	subclause_toks = []
	subclauses = []
	for i, tok in enumerate(whereTokenList):
		if isinstance(tok, sqlparse.sql.TokenList):
			subclause_toks.append(tok)
		elif isinstance(tok, sqlparse.sql.Token):
			if tok.ttype == Token.Text.Whitespace or (tok.ttype == Token.Keyword and str(tok.value).upper() == 'WHERE'):
				continue
			if tok.ttype == Token.Keyword and str(tok.value).upper() == 'AND':
				subclauses.append(sqlparse.sql.TokenList(subclause_toks))
				subclause_toks = []
			else:
				subclause_toks.append(tok)
		else:
			raise Exception('Not a token type')
	subclauses.append(sqlparse.sql.TokenList(subclause_toks))
	return subclauses

# uses results of whereClauseSplit
# can only handle one OR inside parentheses
def whereSubClauseEval(tokenList, atDict, TGList):
	#if len(tokenList.tokens) > 1:
	#	return None
	result = {}
	old_result = {}
	seen_or = False
	seen_and = False
	in_like = False
	like_list = []
	if isinstance(tokenList.tokens[0], sqlparse.sql.Parenthesis):
		#print 'Parenthesis found!'
		for i, tok in enumerate(tokenList.tokens[0].tokens):
			#print 'Is it a token? ' + str(isinstance(tok, sqlparse.sql.Token))
			#print 'Is it a tokenList? ' + str(isinstance(tok, sqlparse.sql.TokenList))
			if in_like:
				like_list.append(tok)
			if isinstance(tok, sqlparse.sql.TokenList):
				#print 'Found TokenList inside parentheses'
				if isinstance(tok, sqlparse.sql.Comparison):
					#print 'Found comparison inside parentheses'
					if seen_and:
						result = combineResultsAnd(result, comparisonEval(tok, atDict, TGList))
						seen_and = False
					else:
						result = comparisonEval(tok, atDict, TGList)
				elif isinstance(tok, sqlparse.sql.Identifier):
					#print 'Found identifier inside parentheses'
					in_like = True
					like_list.append(tok)
			elif isinstance(tok, sqlparse.sql.Token):
				#print 'Found Token inside parentheses'
				#print str(tok.ttype) + ': ' + str(tok.value)
				if tok.ttype == Token.Text.Whitespace or (tok.ttype == Token.Punctuation and (tok.value == '(' or tok.value == ')')):
					continue
				if tok.ttype == Token.Keyword:
					if str(tok.value).upper() == 'AND':
						if in_like:
							if seen_and:
								result = combineResultsAnd(result, likeEval(like_list, atDict, TGList))
							else:
								result = likeEval(like_list, atDict, TGList)
							in_like = False
							like_list = []
						seen_and = True
					elif str(tok.value).upper() == 'OR':
						if in_like:
							if seen_and:
								result = combineResultsAnd(result, likeEval(like_list, atDict, TGList))
							else:
								result = likeEval(like_list, atDict, TGList)
							in_like = False
							like_list = []
						old_result = result
						result = {}
						seen_or = True
			
			else:
				raise Exception
		if in_like:
			result = likeEval(like_list, atDict, TGList)
		if seen_or:
			result = combineResultsOr(result, old_result)
	elif isinstance(tokenList.tokens[0], sqlparse.sql.Comparison): #basically a leaf
		result = comparisonEval(tokenList.tokens[0], atDict, TGList)
	elif isinstance(tokenList.tokens[0], sqlparse.sql.Identifier): #LIKE? seems to be only case of Identifier outside of Comparison also a leaf
		for i, tok in enumerate(tokenList.tokens[0].tokens):
			like_list.append(tok)
		result = likeEval(like_list, atDict, TGList)
	else:
		raise Exception


	return result


def combineResultsAnd(rDict1, rDict2):
	combined = {}
	keys = {}
	for i in rDict1:
		keys[i] = ''
	for i in rDict2:
		keys[i] = ''
	for i in keys:
		if i in rDict1 and i in rDict2:
			combined[i] = rDict1[i].intersection(rDict2[i])
		elif i in rDict1:
			combined[i] = rDict1[i]
		elif i in rDict2:
			combined[i] = rDict2[i]
	return combined

def combineAndList(dList):
	combined = {}
	for d in dList:
		combined = combineResultsAnd(combined,d)
	return combined

def combineResultsOr(rDict1, rDict2): 
	combined = {}
	keys = {}
	for i in rDict1:
		keys[i] = ''
	for i in rDict2:
		keys[i] = ''
	if len(keys) == 1:
		for i in keys:
			if i in rDict1 and i in rDict2:
				combined[i] = rDict1[i].union(rDict2[i])
	return combined

def likeEval(likeList, atDict, TGList):
	table = ''
	alias = ''
	attribute = ''
	is_really_like = False
	value = None
	#print type(comparisonTokenList)
	#print type(comparisonTokenList.tokens[0])
	#print likeList
	#print 'In likeEval...'
	for i, tok in enumerate(likeList):
		#print tok.value
		if isinstance(tok, sqlparse.sql.TokenList):
			for j,tkn in enumerate(tok.tokens):
				if isinstance(tkn, sqlparse.sql.Token):
					if tkn.ttype == Token.Text.Whitespace:
						continue
					if tkn.ttype == Token.Punctuation and tkn.value == '.':
						aliasTok = tok.token_prev(j)[1]
						attribTok = tok.token_next(j)[1]
						if isinstance(aliasTok, sqlparse.sql.Token) and isinstance(attribTok, sqlparse.sql.Token)\
							and aliasTok.ttype == Token.Name and attribTok.ttype == Token.Name:
							alias = str(aliasTok.value)
							table = atDict[alias]
							attribute = str(attribTok.value)
							#print 'alias: ' + alias + ', table: ' + table + ', attribute: ' + attribute
				else:
					raise Exception
				
		elif isinstance(tok, sqlparse.sql.Token):
			#print '..token in likeEval'
			if tok.ttype == Token.Text.Whitespace:
				continue
			if tok.ttype == Token.Keyword and str(tok.value).upper() == 'LIKE':
				is_really_like = True
			elif tok.ttype == Token.Literal.String.Single:
				value = str(tok.value)[1:-1]
		else:
			raise Exception
	if not is_really_like:
		raise Exception('likeEval called but not a LIKE statement')
	#print 'table: ' + str(table)
	#print 'attribute: ' + str(attribute)
	#print 'value: ' + str(value)
	treegroup = None
	for t in TGList:
		if t.FileName == table and t.AttributeName == attribute:
			treegroup = t
			break
	pointerset = getPointersFromLike(treegroup,value)
	return {alias:pointerset}


def getPointersFromLike(treegroup,value):
	#list(t.keys(min=1, max=5, excludemin = True, excludemax= False))
	val_as_regex = re.escape(str(value))
	val_as_regex = re.sub('%', '.*', value)
	val_as_regex = re.sub('_', '.', val_as_regex)
	val_as_regex = '^' + val_as_regex + '$'
	#print val_as_regex
	result = re.match(r'([^%_]*)',value)
	before_wildcard = result.group(1)
	#print before_wildcard
	pointerset = set([])
	if before_wildcard != '':
		max_range = before_wildcard[:-1] + chr(ord(before_wildcard[-1])+1)
		#print max_range
		keylist = treegroup.Keys.keys(min=before_wildcard, max=max_range, excludemin = False, excludemax = True)
		#print 'Preliminary keylist: ' + str(list(keylist))
		for k in keylist:
			#print 'checking ' + str(k)
			#print val_as_regex
			#print str(k)
			if re.match(val_as_regex, str(k)) is not None:
				pointerset = pointerset.union(set(treegroup.Keys[k]))
				#print 'match!'
	else: #you screwed up
		for k in treegroup.Keys:
			if re.match(val_as_regex, str(k)) is not None:
				pointerset = pointerset.union(set(treegroup.Keys[k]))

	return pointerset

def comparisonEval(comparisonTokenList, atDict, TGList):
	table = ''
	alias = ''
	attribute = ''
	operator = ''
	value = None
	#print type(comparisonTokenList)
	#print type(comparisonTokenList.tokens[0])
	for i, tok in enumerate(comparisonTokenList.tokens):
		#print tok.ttype
		if isinstance(tok, sqlparse.sql.TokenList):
			for j,tkn in enumerate(tok.tokens):
				if isinstance(tkn, sqlparse.sql.Token):
					if tkn.ttype == Token.Text.Whitespace:
						continue
					if tkn.ttype == Token.Punctuation and tkn.value == '.':
						aliasTok = tok.token_prev(j)[1]
						attribTok = tok.token_next(j)[1]
						if isinstance(aliasTok, sqlparse.sql.Token) and isinstance(attribTok, sqlparse.sql.Token)\
							and aliasTok.ttype == Token.Name and attribTok.ttype == Token.Name:
							alias = str(aliasTok.value)
							table = atDict[alias]
							attribute = str(attribTok.value)
				else:
					raise Exception
				
		elif isinstance(tok, sqlparse.sql.Token):
			if tok.ttype == Token.Text.Whitespace:
				continue
			if tok.ttype == Token.Operator.Comparison:
				operator = tok.value
			elif tok.ttype == Token.Literal.Number.Integer:
				value = int(tok.value)
			elif tok.ttype == Token.Literal.String.Single or tok.ttype == Token.Literal.String.Symbol:
				value = str(tok.value)[1:-1]
			
		else:
			raise Exception
	#print 'table: ' + str(table)
	#print 'attribute: ' + str(attribute)
	#print 'operator: ' + str(operator)
	#print 'value: ' + str(value)
	treegroup = None
	for t in TGList:
		if t.FileName == table and t.AttributeName == attribute:
			treegroup = t
			break
	#print treegroup.Keys.get('5', None)
	pointerset = getPointersByComparison(treegroup,operator,value)
	return {alias:pointerset}

def getPointersByComparison(treegroup, operator, value):
	# list(t.values(min=1, max=5, excludemin = True, excludemax= False))
	listoflists= []
	if operator == '=':
		check = treegroup.Keys.get(value,None)
		if check is not None:
			listoflists.append(check)
	elif operator == '<>':
		listoflists = list(treegroup.Keys.values(max=value, excludemax=True)) + list(treegroup.Keys.values(min=value, excludemin=True))
	elif operator == '<':
		listoflists = list(treegroup.Keys.values(max=value, excludemax=True))
	elif operator == '<=':
		listoflists = list(treegroup.Keys.values(max=value))
	elif operator == '>':
		listoflists = list(treegroup.Keys.values(min=value, excludemin=True))
	elif operator == '>=':
		listoflists = list(treegroup.Keys.values(min=value))
	else:
		raise Exception

	pointerset = set([])
	for l in listoflists:
		pointerset = pointerset.union(set(l))
	return pointerset

# returns a tuple with a list of tables to join, and a 
def joinClauseEval(result):
	join_idxs = []
	on_idxs = []
	for i, tok in enumerate(result[0].tokens):
		if isinstance(tok, sqlparse.sql.Token) and tok.ttype == Token.Keyword:
			if str(tok.value).upper() == 'JOIN':
				join_idxs.append(i)
			elif str(tok.value).upper() == 'ON':
				on_idxs.append(i)
	if len(join_idxs) == 0:
		print 'No join clause found'
		return None
	if len(on_idxs) != len(join_idxs):
		print 'Invalid join statement'
		return None
	tables_to_join = []
	for idx in join_idx:
		ident = result[0].token_prev(idx)[1]
		tables_to_join.append(getTableFromIdentObj(ident))
	ident = result[0].token_next(join_idx[-1]) # also get table mentioned after last JOIN
	tables_to_join.append(getTableFromIdentObj(ident))

	return tables_to_join

def extractOnConditions(sqlStr, inequalities = False):
	result = sqlparse.parse(sqlStr)
	return extractOnConditionsFromParsed(result)

def extractOnConditionsFromParsed(parseResult, inequalities = False):
	#result = sqlparse.parse(sqlStr)
	at_dict = getTablesAndAliases(parseResult)
	in_from_clause = False
	in_on_clause = False
	compare_list = []
	for i, tok in enumerate(parseResult[0].tokens):
		if isinstance(tok,sqlparse.sql.Where):
			break
		if not in_from_clause: #continue until FROM
			if tok.ttype == Token.Keyword and str(tok.value).upper() == 'FROM':
				#print 'FROM'
				in_from_clause = True
				continue
			else:
				continue
		elif isinstance(tok,sqlparse.sql.Comparison):
			if in_on_clause:
				compare_list.append(tok)
		elif tok.ttype == Token.Whitespace:
			continue
		elif tok.ttype == Token.Keyword:
			if str(tok.value).upper() == 'ON':
				#print 'ON'
				in_on_clause = True
				continue
			elif str(tok.value).upper() == 'JOIN':
				#print 'JOIN'
				in_on_clause = False
				continue
			elif str(tok.value).upper() == 'AND':
				#print 'AND'
				continue
	#print 'num comparisons found: ' + str(len(compare_list))
	TRList = TRsFromComparisons(compare_list, at_dict, inequalities)
	return TRList

def TRsFromComparisons(compList, atDict, inequalities = False):
	TRList = []
	for tok in compList:
		#print tok
		if isinstance(tok, sqlparse.sql.Comparison):
			#print 'is sqlpars.sql.Comparsion'
			for i, tkn in enumerate(tok.tokens):
				#print tkn.ttype
				#print tkn.value
				if tkn.ttype == Token.Operator.Comparison:
					#print 'is comparison'
					if (inequalities and tkn.value != '=') or (not inequalities and tkn.value == '='):
						#print 'got through the gate'
						TR = tupleResults()
						TR.operator = str(tkn.value)
						ident1 = tok.token_prev(i)[1]
						ident2 = tok.token_next(i)[1]
						TR.alias.append(str(ident1.tokens[0].value))
						TR.attribute.append(str(ident1.tokens[2].value))
						TR.fName.append(atDict[TR.alias[0]])
						TR.alias.append(str(ident2.tokens[0].value))
						TR.attribute.append(ident2.tokens[2].value)
						TR.fName.append(atDict[TR.alias[1]])
						
						#self.fName = []#need 2 for joins
       					#self.alias = []#need 2 for joins
        				#self.attribute = []#need 2 for joins
						TRList.append(TR)
		else:
			raise Exception('non-Comparison in token : ' + str(tok))
	return TRList

def postJoinFiltering(parseResult, joinTups, joinTR, ri):
	print 'postJoinFiltering'
	TRList = extractOnConditionsFromParsed(parseResult, inequalities = True)
	#sorted_by_first = sorted(joinResults, key=lambda tup: tup[0])
	output_rows = []
	fs = []
	hs = []
	for i in range(0,len(joinTR.fName)):
		fs.append(open('./source_files/' + joinTR.fName[i], 'rb'))
		hs.append(buildHeaderDict(fs[i]))
	#lot = [(1,2),(1,4)]
	jtr_alias_dict = {}
	for i in range(0, len(joinTR.alias)):
		jtr_alias_dict[joinTR.alias[i]] = i
	orPairs = whereClauseSecondPass(parseResult)
	first = None
	for tup in joinTups:
		rows = []
		for i in range(0, len(tup)):
			#if i == 0:
				#if tup[i] == first:
				#	rows.append(rows[i-1])
				#	continue #save some disk reads, assuming sorted on tup[0]
				#else:
				#	first = tup[i]
			rows.append(lookupRow(fs[i], tup[i], joinTR.fName[i], ri))
		accept_tuple = True
		for tr in TRList:
			i = jtr_alias_dict[tr.alias[0]]
			j = jtr_alias_dict[tr.alias[1]]
			val1 = rows[i][hs[i][tr.attribute[0]]] 
			val2 = rows[j][hs[j][tr.attribute[1]]] 
			if val1 == '' or val2 == '':
				accept_tuple = False
				break
			if tr.operator == '<>':
				if not (val1 != val2):
					accept_tuple = False
					break
			elif tr.operator == '<':
				if not (val1 < val2):
					accept_tuple = False
					break
			elif tr.operator == '<=':
				if not (val1 <= val2):
					accept_tuple = False
					break
			elif tr.operator == '>':
				if not (val1 > val2):
					accept_tuple = False
					break
			elif tr.operator == '>=':
				if not (val1 >= val2):
					accept_tuple = False
					break
			else:
				raise Exception
		if not accept_tuple:
			continue
		for or_pair in orPairs:
			i = jtr_alias_dict[or_pair[0].alias]
			j = jtr_alias_dict[or_pair[1].alias]
			val1 = rows[i][hs[i][or_pair[0].attribute]]
			val2 = rows[j][hs[j][or_pair[1].attribute]]
			eval1 = CBEval(or_pair[0], val1)
			eval2 = CBEval(or_pair[1], val2)
			if not (eval1 or eval2):
				accept_tuple = False
				break
		if accept_tuple:
			full_row = []
			for i in range(0, len(tup)):
				full_row.append(rows[i])
			output_rows.append(full_row)
	return output_rows

def applyProjection(outputRows, TR, parseResult):
	print 'applyProjection'
	#result = sqlparse.parse(sql)
	idents = []
	for tok in parseResult[0].tokens:
		if tok.ttype == Token.Keyword and str(tok.value).upper() == 'FROM':
			break
		if tok.ttype == Token.Wildcard and str(tok.value) == '*':
			idents = None
			break
		if isinstance(tok, sqlparse.sql.IdentifierList):
			for tkn in tok.tokens:
				if isinstance(tkn, sqlparse.sql.Identifier):
					idents.append(tkn)
		elif isinstance(tok, sqlparse.sql.Identifier):
			idents.append(tok)

	if idents is None:
		final_header = []
		for i in range(0, len(TR.alias)):
			f = open('./source_files/' + TR.fName[i], 'rb')
			reader = csv.reader(f)
			hrow = reader.next()
			f.close()
			for h in hrow:
				final_header.append(str(TR.alias[i]) + '.' + str(h))
		final_output = [final_header]
		
		for r in outputRows:
			row = []
			for sr in r:
				row.extend(sr)
			final_output.append(row)

		return final_output
	else:
		proj_args = []
		for tok in idents:
			alias = str(tok.tokens[0].value)
			attrib = str(tok.tokens[2].value)
			proj_args.append(((alias, attrib)))
		final_header = []
		hdict = [] #list of dicts
		adict = {} #just 1 dict
		for i in range(0, len(TR.alias)):
			#print 'File: ' + TR.fName[i]
			f = open('./source_files/' + TR.fName[i], 'rb')
			hdict.append(buildHeaderDict(f))
			f.close()
			adict[TR.alias[i]] = i
		#print hdict


		 
		for pa in proj_args:
			if pa[1] != '*':
				final_header.append(pa[0] + '.' + pa[1])
			else:
				for i in range(0, len(TR.alias)):
					if TR.alias[i] == pa[0]:
						
						f = open('./source_files/' + TR.fName[i], 'rb')
						reader = csv.reader(f)
						hrow = reader.next()
						f.close()
						for h in hrow:
							final_header.append(str(TR.alias[i]) + '.' + str(h))
		final_output = [final_header]
		for r in outputRows:
			#print r
			row = []
			for pa in proj_args:
				i = adict[pa[0]]
				if pa[1] == '*':
					row.extend(r[i])
				else:
					j = hdict[i][pa[1]]
					#print r[i][j]
					row.append(r[i][j])
					#print row
			final_output.append(row)
	return final_output



def lookupRow(f, rowNum, fileName, ri):
	lo_idx = -1
	for i in range(0, len(ri.fName)):
		if ri.fName[i] == fileName:
			lo_idx = i
			break
	#print 'line offset index is ' + str(lo_idx)
	if lo_idx == -1:
		raise Exception('not finding correct line_offset')
	#print 'lo_idx: ' + str(lo_idx)
	#print 'len(ri.line_offset): ' + str(len(ri.line_offset))
	#print 'rowNum: ' + str(rowNum)
	#print 'len(ri.line_offset[lo_idx]): ' + str(len(ri.line_offset[lo_idx]))

	r = csv.reader(f)
	f.seek(ri.line_offset[lo_idx][rowNum])
	return r.next()

def whereClauseSecondPass(parseResult):
	#result = sqlparse.parse(sql)
	at_dict = getTablesAndAliases(parseResult)
	subclauses = whereClauseSplit(parseResult[0].tokens[-1])
	orPairs = []
	for s in subclauses:
		if isinstance(s.tokens[0], sqlparse.sql.Parenthesis):
			for i, tok in enumerate(s.tokens[0].tokens):
				if tok.ttype == Token.Keyword and str(tok.value).upper() == 'OR':
					cmp1 = s.tokens[0].tokens.token_prev(i)
					cmp2 = s.tokens[0].tokens.token_next(i)
					CB1 = CBFromComparison(cmp1)
					CB2 = CBFromComparison(cmp2)
					if CB1.alias != CB2.alias:
						orPairs.append((CB1,CB2))
	return orPairs


def CBFromComparison(cmpTokList):
	#class CompBlock():
	#def __init__(self):
	#	self.alias = ''
	#	self.attribute = ''
	#	self.operator = ''
	#	self.value = None
	CB = CompBlock()
	if isinstance(cmpTokList, sqlparse.sql.Comparison):
		#print 'is sqlpars.sql.Comparsion'
		for i, tok in enumerate(cmpTokList.tokens):
			#print tkn.ttype
			#print tkn.value
			if tok.ttype == Token.Operator.Comparison:
				#print 'is comparison'						
				CB.operator = str(tok.value)
				ident = cmpTokList.token_prev(i)[1]
				CB.value = cmpTokList.token_next(i)[1]
				CB.alias = str(ident.tokens[0].value)
				CB.attribute = str(ident.tokens[2].value)
				break

	return CB

def CBEval(CB, value):
	if CB.operator == '=':
		return (value == CB.value)
	elif CB.operator == '<>':
		return (value != CB.value)
	elif CB.operator == '<':
		return (value < CB.value)
	elif CB.operator == '<=':
		return (value <= CB.value)
	elif CB.operator == '>':
		return (value > CB.value)
	elif CB.operator == '>=':
		return (value >= CB.value)
	else:
		raise Exception('invalid operator')


def buildHeaderDict(f):
	reader = csv.reader(f)
	hrow = reader.next()
	hdict = {}
	for i in range(0,len(hrow)):
		#print hrow[i]
		hdict[hrow[i]] = i
	return hdict

def getAliasDictFromSQL(sqlStr):
	result = sqlparse.parse(sqlStr)
	return getTablesAndAliases(result)

#creates the alias to table dictionary
def getTablesAndAliases(result):
	from_idx = -1
	tables = {}
	for i, tok in enumerate(result[0].tokens):
		if isinstance(tok, sqlparse.sql.Token) and tok.ttype == Token.Keyword and str(tok.value).upper() == 'FROM':
			from_idx = i
			break
	if from_idx == -1:
		return None
	for i, tok in enumerate(result[0].tokens[from_idx+1:]):
		if isinstance(tok, sqlparse.sql.Where):
			break
		if isinstance(tok, sqlparse.sql.IdentifierList):
			for tkn in tok.tokens:
				if isinstance(tkn, sqlparse.sql.Identifier):
					getIdentifierTA(tkn, tables)
		elif isinstance(tok, sqlparse.sql.Identifier):
			getIdentifierTA(tok, tables)
	return tables

def getIdentifierTA(identTokenList, dict):
	name = ''
	alias = ''
	for i, tok in enumerate(identTokenList.tokens):
		if isinstance(tok, sqlparse.sql.Identifier):
			for tkn in tok.tokens:
				if tkn.ttype == Token.Name:
					alias = str(tkn.value)
		else:
			if tok.ttype == Token.Punctuation and tok.value == '.':
				fn = identTokenList.token_prev(i)[1]
				ext = identTokenList.token_next(i)[1]
				#print fn
				#print isinstance(fn,sqlparse.sql.Token)
				#print type(fn)
				#print ext
				#print isinstance(ext,sqlparse.sql.Token)
				#print type(ext)
				if isinstance(fn, sqlparse.sql.Token) and isinstance(ext, sqlparse.sql.Token) and fn.ttype == Token.Name and ext.ttype == Token.Name:
					name = str(fn.value) + '.' + str(ext.value)
				else:
					raise Error
	if name == '':
		return
	elif alias != '':
		dict[alias] = name
	else:
		dict[name] = name
	return

#stores index names that need loading into TR
def getIndexNamesToLoad(tokenList, TR, atDict, from_seen = False):
	#result = sqlparse.parse(sqlStr)
	#TR = tupleResults()
	#at_dict = getTablesAndAliases(parseResult)
	for i, tok in enumerate(tokenList.tokens):
		if not from_seen:
			if tok.ttype == Token.Keyword and str(tok.value).upper() == 'FROM':
				from_seen = True		
			continue
		if isinstance(tok, sqlparse.sql.Comparison):
			for tkn in tok.tokens:
				if isinstance(tkn, sqlparse.sql.Identifier):
					alias  = str(tkn.tokens[0].value)
					TR.alias.append(alias)
					TR.fName.append(atDict[alias])
					TR.attribute.append(str(tkn.tokens[2]))
		elif isinstance(tok, sqlparse.sql.Identifier):
			checktok = tokenList.token_next(i)[1]
			if checktok.ttype == Token.Keyword and str(checktok.value).upper() == 'LIKE':
				alias  = str(tok.tokens[0].value)
				TR.alias.append(alias)
				TR.fName.append(atDict[alias])
				TR.attribute.append(str(tok.tokens[2]))
		elif isinstance(tok, sqlparse.sql.TokenList):
			getIndexNamesToLoad(tok, TR, atDict, from_seen = True)

	return

if __name__ == '__main__':
	Main()