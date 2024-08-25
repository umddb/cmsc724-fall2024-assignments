# Example Queries

Here are a few simpler queries against the `stackexchange` database as examples.

1. Write a query to find all Posts who satisfy one of the following conditions:
       - the post title contains 'postgres' and the number of views is at least 50000
       - the post title contains 'mongodb' and the number of views is at least 25000
The match should be case insensitive.
Output columns: Id, Title, ViewCount

```
select Id, Title, ViewCount
from posts
where (lower(title) like '%postgres%' and viewcount >= 50000) or 
      (lower(title) like '%mongodb%' and viewcount >= 25000)
order by Id asc;
```

2. Count the number of the Badges for the user with DisplayName 'JHFB'.
Output columns: Num_Badges

```
select count(*) as Num_Badges
from users join badges on (users.id = badges.userid)
where users.displayname = 'JHFB';
```


3. Find the Users who have received a "Guru" badge, but not a "Curious" badge.
Only report a user once even if they have received multiple badges with the above names.
Output columns: UserId
Order by: UserId ascending

```
(
select userid from badges where name = 'Guru' 
except 
select userid from badges where name ='Curious'
)
order by userid asc;
```

4. "Tags" field in Posts lists out the tags associated with the post in the format "<tag1><tag2>..<tagn>".
Find the Posts with at least 4 tags, with one of the tags being sql-server-2008 (exact match).
Hint: use "string_to_array" and "cardinality" functions.
Output columns: Id, Title, Tags
Order by Id
```
select Id, Title, Tags
from Posts 
where cardinality(string_to_array(tags, '><')) >= 4 and tags like '%<sql-server-2008>%'
order by id asc;
```


5. SQL "with" clause can be used to simplify queries. It essentially allows
specifying temporary tables to be used during the rest of the query. See Section
3.8.6 (6th Edition) for some examples.

Write a query to find the name(s) of the user(s) with the largest number of badges. 
We have provided a part of the query to build a temporary table.

Output columns: Id, DisplayName, Num_Badges
Order by Id ascending (there may be more than one answer)
```
with temp as (
        select Users.Id, DisplayName, count(*) as num_badges 
        from users join badges on (users.id = badges.userid)
        group by users.id, users.displayname)
select id, displayname, num_badges
from temp 
where num_badges = (select max(num_badges) from temp)
order by Id;
```

6. "With" clauses can be chained together to simplify complex queries. 

Write a query to associate with each user the number of posts they own as well as the
number of badges they have received, assuming they have at least one post and
one badge. We have provided a part of the query to build two temporary tables.

Restrict the output to users with id less than 100.

Output columns: Id, DisplayName, Num_Posts, Num_Badges
Order by Id ascending

```
with temp1 as (
        select owneruserid, count(*) as num_posts
        from posts
        group by owneruserid),
temp2 as (
        select userid, count(*) as num_badges
        from badges
        group by userid)
select users.id, displayname, num_posts, num_badges
from users, temp1, temp2 
where users.id = temp1.owneruserid and users.id = temp2.userid and users.id < 100
order by users.id;
```

7. A problem with the above query is that it may not include users who have no posts or no badges.
Use "left outer join" to include all users in the output.

Feel free to use "with" clauses to simplify the query if you like.

Output columns: Id, DisplayName, Num_Posts, Num_Badges
Order by Id ascending

```
with temp1 as (
        select owneruserid, count(*) as num_posts
        from posts
        group by owneruserid),
temp2 as (
        select userid, count(*) as num_badges
        from badges
        group by userid)
select users.id, displayname, coalesce(num_posts, 0), coalesce(num_badges, 0)
from users left outer join temp1 on (users.id = temp1.owneruserid) 
           left outer join temp2 on (users.id = temp2.userid)
where users.id < 100
order by users.id;
```

8. List the users who have made a post in 2009.
Hint: Use "in".

Output Columns: Id, DisplayName
Order by Id ascending

```
select Id, DisplayName
from users
where id in (select owneruserid 
        from posts 
        where creationdate >= '2009-01-01' and creationdate < '2010-01-01');
```
