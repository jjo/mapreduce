// vim: et si ts=4 sw=4

// Return the shannon entropy for the string sequence,
// based on the probability of each entry vs all
def entropyMap(itemCounts: Map[String, Int]): Double = {
    val totalCount = itemCounts.foldLeft(0)((a, b) => a + b._2)
	val prob = itemCounts.map(r => r._2.toDouble / totalCount)
	return -prob.map(r => r * math.log(r) / math.log(2.0)).reduce(_ + _)
}
// Return (ip, path) from apache logline
def logline_extract(line: String): (String, String) = {
    val log_re = """^(\S+).*GET (\S+).*""".r
    return line match {
        case log_re(ip, path) => (path, ip)
        case _ => ("None","None")
    }
}
val file = sc.textFile("/u/data/star_wars_kid.mini.log")

// file to process
val file = sc.textFile("/u/data/star_wars_kid.log")

// main MR: calculate the 'diversity' (proxied by entropy) of each urlpath,
// taking clients' IPs as the key.
// entroPaths will look like: (
//   (18.10143597100515,"/archive/2003/04/29/star_war.shtml ...")
//   ...
//   (0.009113857674291556,"/watch-info ...")
// )


val entroPaths = file.
    map(r => {val x=logline_extract(r);(x._1, Map[String, Int]((x._2, 1)) )}).
    reduceByKey(_ ++ _).
    map(x => ((math floor entropyMap(x._2) * 100) / 100, x._1)).
    reduceByKey(_ + " " + _).cache()

// sort it down, by entropy value
val sortedEntroPaths = entroPaths.sortBy(_._1, ascending=false)
