// vim: et si ts=4 sw=4

// unused
import scala.collection.mutable.HashMap
def entropy(str: String): Double = {
	var charCount = new HashMap[Char, Int]
	for (c <- str) charCount.put(c, charCount.getOrElse(c, 0) + 1)
	val prob = charCount.map(r => r._2.toDouble / str.length)
	return -prob.map(r => r * math.log(r) / math.log(2.0)).reduce(_ + _)
}
// unused
def inet_aton(ip: String): Long = {
    ip.split('.').
        zipWithIndex.foldLeft(0L)(
            (a,b)=>(a + (b._1.toLong << ((3 - b._2)*8))
        )
    )
}

// Return a dictionary with item counts as -> key: cnt
def counter(items: Seq[String]) : Map[String, Int] = {
    items.groupBy(l => l).map(t => (t._1, t._2.length))
}
// Return the shannon entropy for the string sequence,
// based on the probability of each entry vs all
def entropySeq(items: Seq[String]): Double = {
    val itemCounts = counter(items)
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
    map(r => {val x=logline_extract(r);(x._1, new StringBuilder(x._2))}).
    reduceByKey(_.append("|").append(_)).
    map(x => ((math floor entropySeq(x._2.split('|')) * 100) / 100, x._1)).
    reduceByKey(_ + " " + _).cache()

// sort it down, by entropy value
val sortedEntroPaths = entroPaths.sortBy(_._1, ascending=false)
