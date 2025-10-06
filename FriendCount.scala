// File: FriendCountSpark.scala

val inputFile = "file:///home/dtg95/git/Spark/soc-LiveJournal1Adj.txt"
val outputFolder = "file:///home/dtg95/git/Spark/FC_output"
val timeOutputFolder = "file:///home/dtg95/git/Spark/FC_output_time"

// Record start time
val start = System.nanoTime()

// Step 1: Read input file
val input = sc.textFile(inputFile)

// Step 2: Compute number of friends for each user
val friendCounts = input.map(line => {
  val parts = line.trim.split("\\s+", 2)
  if (parts.length < 2 || parts(1).trim.isEmpty) 0
  else parts(1).split(",").count(_.trim.nonEmpty)
})

// Step 3: Map to (friendCount, 1) for aggregation
val pairs = friendCounts.map(count => (count, 1))

// Step 4: Aggregate by key to get total users per friend count
val distribution = pairs.reduceByKey(_ + _).sortByKey()

// Step 5: Format output
val output = distribution.map { case (count, total) => s"$count\t$total" }

// Step 6: Save friend count output
output.saveAsTextFile(outputFolder)

// Record end time
val end = System.nanoTime()
val duration = (end - start) / 1e9

// Print execution time to console
println(s"Execution time: $duration seconds")

// Step 7: Save execution time to a separate file
sc.parallelize(Seq(s"Execution time: $duration seconds"))
  .saveAsTextFile(timeOutputFolder)
