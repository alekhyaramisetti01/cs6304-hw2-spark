import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}

// ---------- Paths (edit if your username/dirs differ) ----------
val inputPath   = "hdfs://localhost:9000/user/ar89z/InputFolderSpark/soc-LiveJournal1Adj.txt"
val outAllPath  = "hdfs://localhost:9000/user/ar89z/OutputInDegreeAll/"       // all users' indegree
val outTopPath  = "hdfs://localhost:9000/user/ar89z/OutputInDegreeTop100/"    // top 100

// ---------- Parser: "user <TAB/SPACE> f1,f2,..." -> (user, friends[]) ----------
def parseAdjacency(line: String): (Int, Array[Int]) = {
  val t = line.trim
  if (t.isEmpty) return (0, Array.emptyIntArray)
  val parts = t.split("\\s+", 2)
  val user  = parts(0).toInt
  val friends =
    if (parts.length < 2 || parts(1).trim.isEmpty) Array.emptyIntArray
    else parts(1).split(",").iterator.map(_.trim).filter(_.nonEmpty).map(_.toInt).toSet.toArray
  (user, friends)
}

// ---------- Load ----------
val lines: RDD[String] = sc.textFile(inputPath)

// ---------- In-degree counts: (friend, total) ----------
val indegCounts: RDD[(Int, Long)] = lines
  .map(parseAdjacency)
  .flatMap { case (_, friends) => friends.map(f => (f, 1L)) }
  .reduceByKey(_ + _)
  .cache()

// ---------- OUTPUT 1: All users' in-degree (TSV, single file) ----------
val fs = FileSystem.get(sc.hadoopConfiguration)
val pAll = new Path(outAllPath)
if (fs.exists(pAll)) fs.delete(pAll, true)

indegCounts
  .map { case (u, c) => s"$u\t$c" }    // TSV: user<TAB>count
  .coalesce(1, shuffle = true)         // ensure a single part file
  .saveAsTextFile(outAllPath)

// ---------- OUTPUT 2: Top-100 by (count DESC, userId ASC) ----------
/*
  We sort by a composite key (count, -user) and take 100:
    - Primary: higher count first
    - Tie-break: smaller userId first  (because -user is larger when userId is smaller)
*/
val keyed: RDD[((Long, Int), Int)] = indegCounts.map { case (u, c) => ((c, -u), u) }
val top100Keyed: Array[((Long, Int), Int)] = keyed.sortByKey(false).take(100)
val top100: Array[(Int, Long)] = top100Keyed.map { case ((c, _), u) => (u, c) }

val pTop = new Path(outTopPath)
if (fs.exists(pTop)) fs.delete(pTop, true)

sc.parallelize(top100.toSeq, 1)
  .map { case (u, c) => s"$u\t$c" }    // TSV: user<TAB>count
  .saveAsTextFile(outTopPath)

// (Optional) quick print so you see something in the REPL
println("Top 10 preview (user\tinDegree):")
top100.take(10).foreach { case (u, c) => println(s"$u\t$c") }
