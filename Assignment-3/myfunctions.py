import json
import re

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
    global dummyrdd
    dummyrdd = rdd

def task1(postsRDD):
    return postsRDD.filter(lambda t: t['tags'] is not None and '<postgresql-9.4>' in t['tags']).map(lambda t: (t['id'], t['title'], t['tags']))


def task2(postsRDD):
    return postsRDD.filter(lambda t: t['tags'] is not None).flatMap(lambda t: [(t['id'], x) for x in t['tags'][1:-1].split('><')])

def task3(postsRDD):
    def map_f(t):
        return (t['creationdate'][0:4], sorted(t['tags'][1:-1].split('><'))[0:5])
    def reduce_f(l1, l2):
        return sorted(set(l1).union(set(l2)))[0:5]
    return postsRDD.filter(lambda t: t['tags'] is not None).map(map_f).reduceByKey(reduce_f)

def task4(usersRDD, postsRDD):
    u1 = usersRDD.map(lambda t: (t['id'], t['displayname']))
    p1 = postsRDD.map(lambda t: (t['owneruserid'], (t['id'], t['title'])))
    return u1.join(p1).map(lambda t: (t[0], t[1][0], t[1][1][0], t[1][1][1]))

def task5(postsRDD):
    def f(t):
        return [((x, y), 1) for x in t['title'].split() for y in t['tags'][1:-1].split('><')]
    return postsRDD.filter(lambda t: t['tags'] is not None).flatMap(f).reduceByKey(lambda x, y: x+y)


def task6(amazonInputRDD):
    def f(line):
        sp = line.split()
        return (sp[0][4:], sp[1][7:], sp[2])
    return amazonInputRDD.map(f)

def task7(amazonInputRDD):
    def f(line):
        sp = line.split()
        return (sp[0], float(sp[2]))
    return amazonInputRDD.map(f).aggregateByKey((0, 0), lambda x, y: (x[0] + y, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0]/x[1][1]))

def task8(amazonInputRDD):
    def f(line):
        sp = line.split()
        return (sp[1], sp[2])
    def computemode(arr):
        arr = list(arr)
        maxcount = max([arr.count(x) for x in set(arr)])
        for x in ['5.0', '4.0', '3.0', '2.0', '1.0']:
            if arr.count(x) == maxcount:
                return x
    return amazonInputRDD.map(f).groupByKey().mapValues(computemode)

def task9(logsRDD):
        def extractYear(logline):
                match = re.search('/([0-9][0-9][0-9][0-9]):', logline)
                return (match.group(1), 1)
        return logsRDD.map(extractYear).reduceByKey(lambda x, y: x+y)

def task10_flatmap(line):
        return [x for x in re.sub(r'[^a-zA-Z0-9 ]+', '', line).strip().split(" ")]

def task11(playRDD):
        def f(line):
                sp = line.split()
                return (sp[0], (line, len(sp)))
        return playRDD.map(f).filter(lambda x: x[1][1] > 10)


def task12(nobelRDD):
        return nobelRDD.flatMap(lambda x: [(x['category'], d['surname']) for d in x['laureates']]).groupByKey().mapValues(list)

def task13(logsRDD, l):
        def extractHost(logline):
                match = re.search('^(\S+) ', logline)
                return match.group(1) if match is not None else None
        r1 = logsRDD.map(lambda s: (extractHost(s), [d in s for d in l]))
        #r2 = r1.reduceByKey(lambda x1, x2: (x1[0] or x2[0], x1[1] or x2[1]))
        r2 = r1.reduceByKey(lambda x1, x2: tuple([x1[i] or x2[i] for i in range(0, len(l))]))
        return r2.filter(lambda x: all(v for v in x[1])).map(lambda x: x[0])

def task14(logsRDD, day1, day2):
     def findurls(l):
                match = re.search('^(\S+) - - \[\S+ \S+\] "\S+ (\S+) .*', l)
                return (match.group(1), match.group(2))
     r1 = logsRDD.filter(lambda s: day1 in s).map(findurls)
     r2 = logsRDD.filter(lambda s: day2 in s).map(findurls)
     return r1.cogroup(r2).map(lambda x: (x[0], (list(x[1][0]), list(x[1][1])))).filter(lambda x: len(x[1][0]) != 0 and len(x[1][1]) != 0)



def task15(bipartiteGraphRDD):
        degrees = bipartiteGraphRDD.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
        return degrees.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)

def task16(nobelRDD):
        def find_bigrams(s):
                words = s.split(" ")
                return [(words[i], words[i+1]) for i in range(0, len(words)-1)]
        return nobelRDD.flatMap(lambda s: s['laureates']).filter(lambda s: 'motivation' in s).map(lambda s: s['motivation']).flatMap(find_bigrams).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
