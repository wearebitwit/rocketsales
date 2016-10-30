import sys
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from struct import *
import json
import re
import mysql.connector

cassandraHost='127.0.0.1'
mysqlHost='localhost'
mysqlPassword='test'

cnx=mysql.connector.connect(host=mysqlHost,database='batch9',user='test',password=mysqlPassword)
cursor = cnx.cursor()
insertCnx=mysql.connector.connect(host=mysqlHost,database='batch9',user='test',password=mysqlPassword)
insertCursor = insertCnx.cursor()

auth_providers = PlainTextAuthProvider( username=str("dbroot"),password=str("sample"))
cluster = Cluster(port=int(9042),auth_provider = auth_providers, contact_points=[cassandraHost])
session = cluster.connect("feeds")

cursor.execute("truncate slug_map",())

query="""select examid,meta,examname from exam"""
statement = SimpleStatement(query)
insertQuery="""insert into slug_map(slug,type,id,parentId,shortUrl) values(%s,%s,%s,%s,%s)"""
insertStatement = insertQuery


examSlugArray={}
groupSlugArray={}
subjectSlugArray={}
record =session.execute(statement,[])
for row in record:
        #rowmeta=json.loads(row.meta)
        try :
        if 'slug' in row.meta:
                    slug=row.meta['slug']
                    examSlugArray[str(row.examid)]=row.meta['slug']
            insertCursor.execute(insertStatement,(slug,"examCategory",str(row.examid),None,None))
    except TypeError:
        groupname=row.examname.strip(" ");
        groupname = re.sub('[^0-9a-zA-Z]+', '-', groupname).lower()
        groupname=groupname.strip("-");
        slug=groupname;
        examSlugArray[str(row.examid)]=slug;
        insertCursor.execute(insertStatement,(slug,"examCategory",str(row.examid),None,None))

query="""select groupid,groupname,examid,shorterid from group"""


statement = SimpleStatement(query)
record =session.execute(statement,[])
for row in record:
    groupname=row.groupname.strip(" ");
    groupname = re.sub('[^0-9a-zA-Z]+', '-', groupname).lower()
    groupname=groupname.strip("-");
    if row.examid==None:
        continue
    try:
        slug=examSlugArray[str(row.examid)]+"/"+groupname
        insertCursor.execute(insertStatement,(slug,"exam",str(row.groupid),str(row.examid),"grdp.co/g"+row.shorterid));
        groupSlugArray[str(row.groupid)]=slug;
    except Exception:
        print "error"

query= "(select id,name,examId,shortId from categoryHierarchy )";
cursor.execute(query, ());
for row in cursor:
    slug=row[1].strip(" ");
    slug= re.sub('[^0-9a-zA-Z]+', '-', slug).lower()
    slug=slug.strip("-");
    examId=str(row[2])

    if examId in examSlugArray:
        #print str(row[0])
        try:
            slug=examSlugArray[examId]+"/"+slug;
            insertCursor.execute(insertStatement,(slug,"subject",str(row[0]),examId,"grdp.co/c"+row[3]));
            subjectSlugArray[str(row[0])]=slug;
        except Exception:
                    print "error"
    else:
        print "error not found"


query= "(select id,name,examId,shorterId from hierarchyLocal )";
cursor.execute(query, ());
for row in cursor:
        slug=row[1].strip(" ");
        slug= re.sub('[^0-9a-zA-Z]+', '-', slug).lower()
        slug=slug.strip("-");
        examId=str(row[2])

        #if examId in examSlugArray:
                #print str(row[0])
                #try:
                        #insertCursor.execute(insertStatement,(examSlugArray[examId]+"/"+slug,"hierarchyLocal",str(row[0])));
                #except Exception:
                #        print "error"
        #else:
        #        print "error not found"

query= "(select id,listName,examId,shorterId from featureLists )";
cursor.execute(query, ());
for row in cursor:
        slug=row[1].strip(" ");
        slug= re.sub('[^0-9a-zA-Z]+', '-', slug).lower()
        slug=slug.strip("-");
        examId=str(row[2])

        if examId in examSlugArray:
                #print str(row[0])
                try:
                        insertCursor.execute(insertStatement,(examSlugArray[examId]+"/"+slug,"list",str(row[0]),examId,"grdp.co/l"+row[3]));
                except Exception:
                        print "error"
        else:
                print "error not found"


query= "(select boxId,name,entityId,entityType from NBox )";
cursor.execute(query, ());


for row in cursor:
        slug=row[1].strip(" ");
        slug= re.sub('[^0-9a-zA-Z]+', '-', slug).lower()
        slug=slug.strip("-");
        entityId=str(row[2])
    print row[2]
    entityType=str(row[3])
    if entityType=="0":
        if entityId in subjectSlugArray:
            try:
                insertCursor.execute(insertStatement,(subjectSlugArray[entityId]+"/"+slug,"box",str(row[0]),entityId,None));
            except Exception:
                print "error"
        if entityType=="1":
                if entityId in groupSlugArray:
                        try:
                                insertCursor.execute(insertStatement,(groupSlugArray[entityId]+"/"+slug,"box",str(row[0]),entityId,None));
                        except Exception:
                                print "error"
    if entityType=="2":
        if entityId in examSlugArray:
                #print str(row[0])
                    try:
                            insertCursor.execute(insertStatement,(examSlugArray[entityId]+"/"+slug,"box",str(row[0]),entityId,None));
                    except Exception:
                            print "error"
        else:
                print "error not found"


insertCnx.commit();
