import psycopg2
import os
import sys
import datetime
from collections import Counter
from types import *
import argparse

from queries import *

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--interactive', help="Run queries one at a time, and wait for user to proceed", required=False, action="store_true")
parser.add_argument('-q', '--query', type = int, help="Only run the given query number", required=False)
args = parser.parse_args()

interactive = args.interactive

conn = psycopg2.connect("dbname=stackexchange user=root")
cur = conn.cursor()

print("Dropping and creating commentscopy, lengthfiltertype")
cur.execute("drop table if exists commentscopy;")
cur.execute("drop type if exists lengthfiltertype;")
cur.execute("create table commentscopy as (select * from comments);")
cur.execute("CREATE TYPE lengthfiltertype AS ENUM ('Long', 'Medium', 'Short')")
conn.commit()

print("Dropping and creating trigger UpdateStarUsers on badges")
cur.execute("drop trigger if exists UpdateStarUsers on badges;")
conn.commit()

print("Dropping and creating tables/views postssummary, starusers, answered")
cur.execute("drop table if exists starusers;")
cur.execute("create table starusers as select u.ID, count(b.ID) as NumBadges from users u left join badges b on u.id = b.userid group by u.id having count(b.ID) > 10;")


cur.execute("drop view if exists postssummary;")
cur.execute("drop table if exists answered;")
cur.execute("create table answered as select p1.owneruserid as parent, p2.owneruserid as child from posts p1, posts p2 where p1.id = p2.parentid and p1.owneruserid != p2.owneruserid;")
conn.commit()

input('Press enter to proceed')

test_queries_to_run = [None] * 100
test_queries_to_run[9] = ("SELECT * FROM commentscopy where id < 10 order by id", 
                                "-- Result (should have appropriate new columns)") 
test_queries_to_run[10] = ("SELECT * FROM commentscopy where id < 10 order by id", 
                                "-- Result (should have appropriate new columns with appropriate values)") 
test_queries_to_run[11] = ("select * from commentscopy where id in (6081, 6251); ",
                                "-- Result should not have any tuples")
test_queries_to_run[12] = ("select * from commentscopy where id between 50000 and 50010", 
                                "-- Result should contain the new 10 tuples along with older tuples with those ids")
test_queries_to_run[13] = ("select * from information_schema.table_constraints tc where tc.table_name = 'commentscopy';", 
       "-- Result (should have the 3 new constraints for commentscopy, but not all the information")

test_queries_to_run[15] = ("select * from PostsSummary where id < 10", 
                                "-- Result (should have 8 rows with appropriate counts)")
test_queries_to_run[17] = ("select id, title, numvotes(id) from posts limit 20", 
                                "-- Result")
test_queries_to_run[18] = ("select userposts(7)", "-- Result")

totalscore = 0
for i in range(0, 21):
    # If a query is specified by -q option, only do that one
    if args.query is None or args.query == i:
        try:
            if interactive:
                os.system('clear')
            print("========== Executing Query {}".format(i))
            print(queries[i])
            cur.execute(queries[i])

            if i in [1, 2, 3, 4, 5, 6, 7, 8, 14, 16, 19, 20]:
                ans = cur.fetchall()

                print("--------- Your Query Answer ---------")
                for t in ans:
                    print(t)
                print("")
            elif i in [9, 10, 11, 12, 13, 15, 17, 18]:
                conn.commit()
                print("--------- Running {} -------".format(test_queries_to_run[i][0]))
                cur.execute(test_queries_to_run[i][0])
                ans = cur.fetchall()
                print(test_queries_to_run[i][1])
                for t in ans:
                    print(t)
                print("")
                
            if interactive:
                input('Press enter to proceed')
                os.system('clear')
        except:
            print(sys.exc_info())
            raise
