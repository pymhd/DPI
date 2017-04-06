import os
import sqlite3


#class __Connection:
#    def __init__()
# connect from module, 1 string

class Storage:
    def __init__(self, Con):
        self.con = Con
        self.cur = Con.cursor()
    
    def add_record(self, type, url, id):
        SQL = "insert into main values ('{0}', '{1}', {2})".format(type, url, id)
        self.cur.execute(SQL)
        self.con.commit()
        return True
    
    def find_record(self, url):
        SQL = "select id from main where url like '%{0}%'".format(url)
        self.cur.execute(SQL)
        try: id = self.cur.fetchall()[0][0]
        except IndexError: return False
        return id

    def get_type(self, url_type):
        SQL = "select url, id from main where type = '{0}'".format(url_type)
        self.cur.execute(SQL)
        rows = self.cur.fetchall()
        return [{"url": row[0], "id": row[-1]} for row in rows if row and row[1] > 100]
        
    