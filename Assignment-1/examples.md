# Example Queries

Here are a few simpler queries against the `stackexchange` database as examples.

1. Write a query to find all Posts who satisfy one of the following conditions:
       - the post title contains 'postgres' and the number of views is at least 50000
       - the post title contains 'mongodb' and the number of views is at least 25000
The match should be case insensitive

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
