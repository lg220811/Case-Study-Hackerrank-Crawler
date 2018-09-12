import requests
import os
import json
import sqlite3
from bs4 import BeautifulSoup

DEBUG = 1
#
TABLE_PROBLEM_NAME = "problem"
PROBLEM_ID = "id"
PROBLEM_NAME = "name"
PROBLEM_CONTENT = "content"
#
TABLE_USER_NAME = "user"
USER_ID = "id"
USER_NAME = "name"
USER_COUNTRY = "country"
#
TABLE_RESULT_NAME = "result"
RESULT_ID = "id"
RESULT_RANK = "rank"
RESULT_PROBLEM_ID = "problem_id"
RESULT_USER_ID = "user_id"
RESULT_SCORE = "score"
RESULT_TIME = "time"
#
class SqliteHandler :
	def __init__(self) : 
		self.con = sqlite3.connect('hackerrank.db')

	def close(self) :
		self.con.close()
	#create table prolem
	def createTableProblem(self):
		cur = self.con.cursor()
		stm = 'CREATE TABLE IF NOT EXISTS ' \
			+ TABLE_PROBLEM_NAME + ' ('\
			+ PROBLEM_ID + ' INTEGER PRIMARY KEY, '\
			+ PROBLEM_NAME + ' TEXT, '\
			+ PROBLEM_CONTENT + ' TEXT)'
		if DEBUG : 
			print(stm)
		cur.execute(stm)
		self.con.commit()
	
	#create table user
	def createTableUser(self):
		cur = self.con.cursor()
		stm = 'CREATE TABLE IF NOT EXISTS '\
			+ TABLE_USER_NAME+' ('\
			+ USER_ID + ' INTEGER PRIMARY KEY, '\
			+ USER_NAME + ' TEXT, '\
			+ USER_COUNTRY + ' TEXT)'
		if DEBUG : 
			print(stm)
		cur.execute(stm)
		self.con.commit()
	
	#create table result
	def createTableResult(self):
		cur = self.con.cursor()
		stm = 'CREATE TABLE IF NOT EXISTS '\
			+ TABLE_RESULT_NAME + ' ('\
			+ RESULT_ID + ' INTEGER PRIMARY KEY, '\
			+ RESULT_RANK + ' INTEGER, '\
			+ RESULT_PROBLEM_ID + ' INTEGER, '\
			+ RESULT_USER_ID + ' INTEGER, '\
			+ RESULT_SCORE + ' REAL, '\
			+ RESULT_TIME + ' INTEGER, '\
			+ 'FOREIGN KEY (' + RESULT_PROBLEM_ID + ') REFERENCES ' + TABLE_PROBLEM_NAME + '(' + PROBLEM_ID + '), '\
			+ 'FOREIGN KEY (' + RESULT_USER_ID + ') REFERENCES ' + TABLE_USER_NAME + '(' + USER_ID + '))'
		if DEBUG : 
			print(stm)
		cur.execute(stm)
		self.con.commit()
	
	def insertProblem(self, id, name, content):
		cur = self.con.cursor()
		stm = "INSERT INTO "+ TABLE_PROBLEM_NAME + " VALUES (?,?,?)"
		if DEBUG : 
			print(stm)
		try:
			cur.execute(stm,[id,name,content])
		except:
			print('Problem already in database')
		self.con.commit()
	
	def insertUser(self, id, name, country):
		cur = self.con.cursor()
		if country :
			country = country
		else :
			country = "Unknown"
		stm = "INSERT INTO "\
			+ TABLE_USER_NAME + " VALUES ("\
			+ str(id) + ",'"\
			+ name + "','"\
			+ country + "')"
		if DEBUG : 
			print(stm)
		try:
			cur.execute(stm)
			self.con.commit()
		except sqlite3.IntegrityError:
			print('User already in database')
	
	def insertResult(self, result_id, rank, problem_id, hacker_id, score, time_taken):
		cur = self.con.cursor()
		stm = "INSERT INTO "\
			+ TABLE_RESULT_NAME + " VALUES ("\
			+ str(result_id) + ","\
			+ str(rank) + ","\
			+ str(problem_id) + ","\
			+ str(hacker_id) + ","\
			+ str(score) + ","\
			+ str(time_taken) + ")"
		if DEBUG : 
			print(stm)
		cur.execute(stm)
		self.con.commit()
		
	def dropTable(self, table):
		cur = self.con.cursor()
		stm = "DROP TABLE "+table
		cur.execute(stm)
		self.con.commit()
		
	def leaderboard(self,id):
		cur = self.con.cursor()
		stm = "SELECT "\
			+ TABLE_RESULT_NAME+"."+RESULT_RANK+","\
			+ TABLE_USER_NAME+"."+USER_NAME+","\
			+ TABLE_USER_NAME+"."+USER_COUNTRY+","\
			+ TABLE_RESULT_NAME+"."+RESULT_SCORE+" "\
			+ "FROM " + TABLE_RESULT_NAME+" "\
			+ "JOIN "+TABLE_USER_NAME+" "\
			+ "ON "+TABLE_RESULT_NAME+"."+RESULT_USER_ID+" "\
			+ "= "+TABLE_USER_NAME+"."+USER_ID+" "\
			+ "WHERE "+TABLE_RESULT_NAME+"."+RESULT_PROBLEM_ID+" "\
			+ "= "+id
		if DEBUG :
			print(stm)
		cur.execute(stm)
		return cur.fetchall()

		
#crawl data from website, should call once
def crawl():
	#init database
	db = SqliteHandler()

	db.createTableProblem()
	db.createTableUser()
	db.createTableResult()


	offset = 0 #start position 
	thresh_hold = 50 #number of problem per request
	count = 0 #current position
	count_result = 0 #key id of result table
	url_problem_list = "https://www.hackerrank.com/rest/contests/master/tracks/algorithms/challenges"

	#start scrap
	while 1 :
		#param pass to GET
		params = {
			"offset" : offset,
			"limit" : thresh_hold
		}
		#request GET
		r = requests.get(url_problem_list, params = params)
		#parse json to object
		data = json.loads(r.text)
		
		#iterate data
		for problem in data['models'] :
			count += 1
			#problem description
			r = requests.get("https://www.hackerrank.com/challenges/" + problem['slug'] + "/problem")
			soup = BeautifulSoup(r.text, "html.parser")
			s = soup.find_all('div', class_='challenge-body-html')
			#insert problem
			db.insertProblem(problem['id'],problem['name'],str(s))
			
			#leaderboard
			params = {
				"offset" : '0',
				"limit" : '100',
				"include_practice" : "true"
			}
			r = requests.get("https://www.hackerrank.com/rest/contests/master/challenges/" + problem['slug'] + "/leaderboard", params = params)
			leaderboard = json.loads(r.text)
			#iterate data
			for user in leaderboard['models'] :
				count_result += 1
				db.insertUser(user['hacker_id'],user['hacker'],user['country'])
				db.insertResult(str(count_result),user['rank'],problem['id'],user['hacker_id'],user['score'],user['time_taken'])
			
		if count < offset + thresh_hold : #reach end of problem list
			break
			
		offset += thresh_hold
		
	db.close()
	
#get leaderboard of problem
def leaderboard(id):
	db = SqliteHandler()
	list = db.leaderboard(id)
	
	for user in list :
		#print(user['rank'],user['name'],user['country'],user['score'])
		print(user)
	db.close()
	
if __name__ == "__main__":
	#crawl()
	leaderboard("41")