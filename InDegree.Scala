package hw2

import org.apache.spark.{SparkConf, SparkContext}

object InDegreeTop10 {

  /** Parse "user<tab_or_space>f1,f2,f3,..." → (userId, friends[]) */
  private def parseAdjacency(line: String): (Int, Array[Int]) = {
    val t = line.trim
    if (t.isEmpty) return (0, Array.emptyIntArray)
    val parts = t.split("\\s+", 2)
    val user = parts(0).toInt
    val friends =
      if (parts.length < 2 || parts(1).trim.isEmpty) Array.emptyIntArray
      else parts(1).split(",")
        .iterator
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(_.toInt)
        .toSet                 // de-dup per line
        .toArray
    (user, friends)
  }

  /**
   * Args:
   *   args(0) = input path (e.g., local: /path/file.txt, or HDFS: hdfs:///user/you/input.txt)
   *   args(1) = output directory (Spark writes a folder with a single TSV part file)
   *
   * Output format (TSV):
   *   userId<TAB>inDegree
   * Only the global Top-10 by in-degree are written (desc). Ties → smaller userId first.
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: InDegreeTop10 <inputPath> <outputDir>")
      System.exit(1)
    }
    val Array(input, outputDir) = args.take(2)

    val conf = new SparkConf().setAppName("InDegreeTop10")
    val sc   = new SparkContext(conf)
    try {
      // Read (local or HDFS) and parse
      val parsed = sc.textFile(input).map(parseAdjacency)

      // Count in-degrees: for each friend f listed by some user u, emit (f, 1)
      val inDegree = parsed
        .flatMap { case (_, friends) => friends.map(f => (f, 1L)) }
        .reduceByKey(_ + _)

      // Top-10 by (count desc, userId asc)
      val top10 = inDegree.top(10)(Ordering.by[(Int, Long), (Long, Int)] {
        case (userId, cnt) => (cnt, -userId)
      })

      // Ensure a single, tab-separated file (part-00000). You can rename it to .tsv after.
      sc.parallelize(top10, 1)
        .map { case (user, cnt) => s"$user\t$cnt" }   // TSV content
        .coalesce(1, shuffle = true)
        .saveAsTextFile(outputDir)
    } finally {
      sc.stop()
    }
  }
}
