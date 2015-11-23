from pyspark import SparkContext, SparkConf

sc = SparkContext(appName="bad/good count")

if __name__ == "__main__":
    #val header = sc.textFile("/user/lidge/pw/ATO_Series.head").collect.head.split("\\|",-1).zipWithIndex.map( _.swap).toMap
    header = sc.textFile("/user/lidge/pw/ATO_Series.head").map(lambda line: dict(enumerate(line.split("|")))).first()
    #val body=sc.textFile("/user/lidge/pw/ATO_Series.body").map( line => line.split("\\|",-1).zipWithIndex.map(x=> (header.getOrElse(x._2,"no_head"), x._1)))
    body = sc.textFile("/user/lidge/pw/ATO_Series.body").map( lambda line: [ (header[ind], value) for (ind, value) in enumerate(line.split("|")) ] )

    bad_tag = "ato_bad_1"
    total_count = body.count()
    #bad_column = body.map(record.filter(x._1 == ato_bad))
    tag_column = body.map( lambda line : [ value for (key, value) in line if key == bad_tag ][0] )
    tag_column.first()
    good_count = tag_column.filter(lambda x: x == '0').count()
    bad_count = tag_column.filter(lambda x: x == '1').count()
    ind_count = tag_column.filter(lambda x: x == '2').count()

    print("total_count: " + str(total_count))
    print("good_count: " + str(good_count))
    print("bad_count: " + str(bad_count))
    print("ind_count: " + str(ind_count))
