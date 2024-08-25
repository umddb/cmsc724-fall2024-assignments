queries = ["" for i in range(0, 17)]

### 0. List all the users who have at least 1000 UpVotes.
### Output columns and order: Id, Reputation, CreationDate, DisplayName
### Order by Id ascending
queries[0] = """
select Id, Reputation, CreationDate,  DisplayName
from users
where UpVotes >= 1000
order by Id asc;
"""

### 1. Find the users who have made a post in 2009 (between 1/1/2009 and 12/31/2009)
### and have received a badge in 2011 (between 1/1/2011 and 12/31/2011).
### Hint: Use "intersect" and "in".
###
### Output Columns: Id, DisplayName
### Order by Id ascending
queries[1] = """
select 0;
"""

### 2. Write a query to output a list of posts with comments, such that PostType = 'Moderator nomination' 
### and the comment has score of at least 10. So there may be multiple rows with the same post
### in the output.
###
### This query requires joining three tables: Posts, Comments, and PostTypes.
###
### Output: Id (Posts), Title, Text (Comments)
### Order by: Id ascending
queries[2] = """
select 0;
"""


### 3. For the users who have received at least 200 badges in total, find the
### number of badges they have received in each year. This can be used, e.g., to 
### create a plot of the number of badges received in each year for the most active users.
###
### There should be an entry for a user for every year in which badges were given out.
###
### We have provided some WITH clauses to help you get started. You may wish to 
### add more (or modify these).
###
### Output columns: Id, DisplayName, Year, Num_Badges
### Order by Id ascending, Year ascending
###
### NOTE: If the query below fails for you for some reason (it seems to be happening on amd64 image),
### use this query instead -- the output is identical but years are hardcoded:
###
### with temp1 as (
###      select id, displayname, year
###      from users, generate_series(2011, 2022) as year
###      where id in (select userid from badges group by userid having count(*) >= 200))
### select * from temp1;
###
queries[3] = """
with years as (
        select distinct extract(year from date) as year 
        from badges),
     temp1 as (
        select id, displayname, year
        from users, years
        where id in (select userid from badges group by userid having count(*) >= 200)
     )
select 0;
"""

### 4. Find the post(s) that took the longest to answer, i.e., the gap between its creation date
### and the creation date of the first answer to it (in number of days). Ignore the posts with no
### answers. Keep in mind that "AcceptedAnswerId" is the id of the post that was marked
### as the answer to the question -- joining on "parentid" is not the correct way to find the answer.
###
### Hint: Use with to create an appropriate table first.
###
### Output columns: Id, Title, Gap
queries[4] = """
select 0;
"""


### 5. Write a query to find the posts with at least 7 children, i.e., at
### least 7 other posts have that post as the parent
###
### Output columns: Id, Title
### Order by: Id ascending
queries[5] = """
select 0;
"""

### 6. Find posts such that, between the post and its children (i.e., answers
### to that post), there are a total of 100 or more votes
###
### HINT: Use "union all" to create an appropriate temp table using WITH
###
### Output columns: Id, Title
### Order by: Id ascending
queries[6] = """
select 0;
"""

### 7. Let's see if there is a correlation between the length of a post and the score it gets.
### We don't have posts in the database, so we will do this on title instead.
### Write a query to find the average score of posts for each of the following ranges of post title length:
### 0-9 (inclusive), 10-19, ...
###
### We will ignore the posts with no title.
###
### HINT: Use the "floor" function to create the ranges.
###
### Output columns: Range_Start, Range_End, Avg_Score
### Order by: Range ascending
queries[7] = """
select 0;
"""


### 8. Write a query to generate a table: 
### (VoteTypeDescription, Day_of_Week, Num_Votes)
### where we count the number of votes corresponding to each combination
### of vote type and Day_of_Week (obtained by extract "dow" on CreationDate).
### So Day_of_Week will take values from 0 to 6 (Sunday to Saturday resp.)
###
### Don't worry if a particular combination of Description and Day of Week has 
### no votes -- there should be no row in the output for that combination.
###
### Output column order: VoteTypeDescription, Day_of_Week, Num_Votes
### Order by VoteTypeDescription asc, Day_of_Week asc
queries[8] = """
select 0;
"""

### 9. Create a copy of the "Comments" table and add different columns.
### select * into CommentsCopy from Comments, OR
### create table CommentsCopy as (select * from Comments)
###
### Also, use the following command to create a new "Type"
### CREATE TYPE LengthFilterType AS ENUM ('Long', 'Medium', 'Short');
###
### For the next few questions, we will use this duplicated table
###
### Write a single query/statement to add three new columns to the "CommentsCopy" table --
### CommentLength (integer), CommenterDisplayName (varchar(40)), and lengthFilter (of type LengthFilterType).
### See: https://www.postgresql.org/docs/current/datatype-enum.html if you are
### unsure how to use the enum type
queries[9] = """
select 0;
"""

### 10. Write a single query/statement to set the values of the new columns.
###
### The "Lengthfilter" column should be set as follows:
### Long: length(text) > 100, Medium: length(text) between 50 and 100,
###         and Low: length(text) < 50
### Use CASE to write this part.
### You may have to do an explicit cast for the popularity attribute using '::lengthfiltertype'
### 
### commenterDisplayName should be obtained from the Users table
###
### https://www.postgresql.org/docs/current/sql-update.html has examples at the
### bottom to show how to update multiple columns in the same query
queries[10] = """
select 0;
"""


### 11. Write a query "delete" all Posts from CommentsCopy where the displayname of the user who 
### posted it contains the word "nick" (case insensitive).
queries[11] = """
select 0;
"""

### 12. Use "generate_series" to write a single statement to insert 10 new tuples
### to 'CommentsCopy' of the form:
### (ID = 50001, 'Comment 50001', PostId = 2, Score = 0, CreationDate = '2022-10-01', UserId = -1)
### (ID = 50002, 'Comment 50002', PostId = 2, Score = 0, CreationDate = '2022-10-02', UserId = -1)
### ...
### (ID = 50010, 'Comment 50010', PostId = 2, Score = 0, CreationDate = '2022-10-10', UserId = -1)
###
### All other attributes should be set to NULL.
###
### HINT: Use concatenation operator: 'Comment' || 0, and addition on dates to simplify.
### 
### Use `select * from commentscopy where id >= 50001 and id <=50010;` to confirm.
queries[12] = """
select 0;
"""


### 13. The above command should not have actually worked, because the "ID" column is
### a primary key, and we are trying to insert duplicate values.
###
### However, because of the way we created the table, the "ID" column is not actually a primary key.
### In fact, none of the constraints from the original "Comments" table were copied over.
###
### You can see this by running: "\d commentscopy" in psql, and looking at the "CONSTRAINT" section
### and also "Nullable" column (and comparing to "\d comments").
###
### Write a single ALTER TABLE command to add a NOT NULL constraint on the "ID" column, add
### a foreign key constraint on the "PostId" column, and add a check constraint on the "Score" column (score >= 0).
###
### Link to documentation: https://www.postgresql.org/docs/current/sql-altertable.html
###
### We can't add a primary key constraint any more, because there are duplicate values in the "ID" column now
### and those would need to be taken care of manually first.
###
queries[13] = """
select 0;
"""

### 14. Write a single query to rank the "Users" by the number of badges, with the User
### with the highest number of badges getting rank 1.
### If there are ties, the two (or more) badges should get the same "rank", and next ranks
### should be skipped.
###
### HINT: Use a WITH clause to create a temporary table (temp(UserID, NumBadges)
### followed by the appropriate "RANK"
### construct -- PostgreSQL has several different
### See: https://www.eversql.com/rank-vs-dense_rank-vs-row_number-in-postgresql/, for some
### examples.
### PostgreSQL documentation has a more succinct discussion: https://www.postgresql.org/docs/current/functions-window.html
###
### Output Columns: UserID, Rank
### Order by: Rank ascending, UserID ascending
queries[14] = """
select 0;
"""


### 15. Write a statement to create a new View with the signature:
### PostsSummary(Id, NumVotes, NumComments)
### 
### Use inline scalar subqueries in "select" clause to simplify this.
###
### This View can be used to more easily keep track of the stats for the posts.
### 
### Confirm that the view is created properly by running:
###      select * from PostsSummary limit 10;
###
### Note that: depending on exactly the query used, a "select * from
### PostsSummary" will likely be very very slow.
### However a query like: select * from PostsSummary where ID = 20;
### should run very fast.
###
### Ensure that the latter is case (i.e., the query for a single ID runs quickly).
queries[15] = """
select 0;
"""

### 16. Use window functions to construct a query to associate with each post
### the average score of posts that are created in the same month.
###
### See here for a tutorial on window functions: https://www.postgresql.org/docs/current/tutorial-window.html
###
### We have created a table using WITH for you: 
###         temp(ID, Title, CreatedYear, CreatedMonth, Score)
### Our goal is to create a new table with columns: 
###      (ID, Title, CreatedYear, CreatedMonth, Score, AvgScoreForPostsFromThatMonth)
### Here: AvgScoreForPostsFromThatMonth is basically the average score of posts across
### that are created in that month
###
### This kind of an output table will allow us to compare each post with the other posts
### which are created in that same month
### Order by: CreatedYear first, CreatedMonth next, and then ID;
###
### Note: We will be checking that you are using window functions.
###
### First few rows would look like this:
### id   |                   title                   | createdyear | createdmonth | score | avgscoreforpostsfromthatmonth
### -------+-------------------------------------------------+------------+-------+------------------------------
### 10034 | How can I learn to become a DBA?                                     |       2009 |    1 |    16 |          12.3333333333333333
### 10038 |                                                                      |       2009 |    1 |    6  |          12.3333333333333333
### 10042 |                                                                      |       2009 |    1 |    15 |          12.3333333333333333
### 17168 | SSAS - Inferred Dimension Attributes - are they necessary?           |       2009 |    5 |    5  |          5.0000000000000000
### 10670 | Which databases support parallel processing across multiple servers? |       2010 |    3 |    5  |          6.6666666666666667
###

queries[16] = """
with temp as (
        select ID, title, extract(year from CreationDate) as CreatedYear, extract(month from CreationDate) as CreatedMonth, score
        from posts
        )
select 0;
"""

### 17. Write a function that takes in the ID of a Post as input, and returns the
### number of votes for that post.
###
### Function signature: NumVotes(in integer, out NumVotes bigint)
###
### There are several examples here at the bottom: https://www.postgresql.org/docs/10/sql-createfunction.html
### You should be writing one that uses SQL, i.e., has "LANGUAGE SQL" at the end.
### 
### So calling NumVotes(20) should return 67. Make sure your function returns 0
### appropriately (for Posts who do not have any Votes).
### 
### Confirm that the query below works after the function is created:
###             select ID, Title, NumVotes(ID) from Posts limit 100
### As for one of the questions above, trying to run this query without "limit" 
### will be very slow given the number of posts.
###
queries[17] = """
select 0;
"""

### 18. Write a function that takes in an Userid as input, and returns a JSON string with 
### the details of Posts owned by that user.
###
### So SQL query: select UserPosts(7);
### should return a single tuple with a single attribute of type string/varchar as:
###  { "userid": 7,
###    "displayname": "Toby",
###   "posts": [ {"title": "How can a group track database schema changes?", "score": 68, "creationdate": "2011-01-03"},
###                {"title": "", "score": 15, "creationdate": "2011-01-03"},
###                {"title": "Confirm that my.cnf file has loaded OK", "score": 4, "creationdate": "2011-01-19"}
###    ]}
###
### The posts should be ordered by "date" first, and then by title of the post.
### 
###
### You should use PL/pgSQL for this purpose -- writing this purely in SQL is somewhat cumbersome.
### i.e., your function should have LANGUAGE plpgsql at the end.
###
### Function signature: UserPosts(in integer, out PostsJSON varchar)
###
### HINT: Use "string_agg" aggregate functions for creating the list of badges properly: https://www.postgresqltutorial.com/postgresql-aggregate-functions/postgresql-string_agg-function/
### Use "FORMAT()" function for constructing the JSON strings -- you can also
### use CONCAT, but FORMAT is more readable: https://www.postgresql.org/docs/current/functions-string.html#FUNCTIONS-STRING-FORMAT
###
### BE CAREFUL WITH WHITE SPACES -- we will remove any spaces before comparing answers, but there is
### still a possibility that you fail comparisons because of that.
### 
### Confirm that: 'select userposts(7);' returns the result above.
queries[18] = """
select 0;
"""

### 19. The goal here is to write a query to find the total view count of posts for each
### tag. However, tags are currently stored in the format "<mysql><version-control><schema>"
### 
### So write a query to first convert the "tags" strings into an array (make
### sure to account for the "<" and ">" characters appropriately), and then
### use "unnest" to create a temporary table with schema: 
### temp(tag, postid, viewcount)
###
### Given this table, we can find the number of total view count of posts for each tag easily.
###
### Output columns: Tag, TotalViewCounts
### Order by: TotalViewCounts descending
###
### Three functions to make this easier: left(), right(), and unnest()
### 
queries[19] = """
select 0;
"""


## 20. Create a new table using the following command:
###    create table Answered as
###       select p1.owneruserid as parent, p2.owneruserid as child
###       from posts p1, posts p2
###       where p1.id = p2.parentid and p1.owneruserid != p2.owneruserid;
###
### This table contains pairs of users where the second user has answered a question by the first user.
### 
### Write a query that uses a recursive CTE to associate each user with all the users who have answered their questions, 
### or answered questions by users who have answered their questions, and so on, and then counts the number of children
### for each user. (This query is a bit artificial as the schema lacks a good recursive structure).
###
### The output should be a table with two columns: parent, ct
### Order by: parent
queries[20] = """
select 0;
"""
