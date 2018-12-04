#!/usr/bin/env python3

import psycopg2
from datetime import datetime

try:
    db = psycopg2.connect("dbname=news")
except psycopg2.Error as e:
    print "Unable to connect!"
    print e.pgerror
    print e.diag.message_detail
    sys.exit(1)
else:
    print "Connected!"


c = db.cursor()
print("1. What are the most popular three articles of all time?")
query1 = """ SELECT title, COUNT(*) AS num
                    FROM articles, log
                    WHERE log.path = '/article/' || articles.slug
                    GROUP BY title
                    ORDER BY num DESC LIMIT 3"""
c.execute(query1)
popular_articales = c.fetchall()
for row in popular_articales:
    print '"{article}" -- {count} views'.format(article=row[0], count=row[1])
print("")
print("2. Who are the most popular article authors of all time?")
query2 = """ SELECT name, COUNT(*) AS sum
                    FROM authors, articles,log
                    WHERE authors.id = articles.author
                    AND  log.path = '/article/' || articles.slug
                    GROUP BY name ORDER BY sum DESC """
c.execute(query2)
popular_author = c.fetchall()
for row in popular_author:
        print(row[0].title()+' -- '+str(row[1])+" views")
print("")
print("3. On which days did more than 1% of requests lead to errors?")
query3 = """ SELECT * from
(SELECT  time,COUNT(*) AS req FROM log GROUP BY time) num_req,
(SELECT  time,COUNT(*) AS count FROM log WHERE status='404 NOT FOUND'
GROUP BY time ORDER BY count DESC limit 3) errors,log
WHERE num_req.time = errors.time
and((errors.count + 0.0) / num_req.req  *100) >1
ORDER BY 1 DESC LIMIT 1"""

c.execute(query3)
errors = c.fetchall()
for row in errors:
        logDate = row[0].date()
        print(logDate.strftime('%B %d, %Y')+" -- "+str(row[1])+"% errors")
db.close()
