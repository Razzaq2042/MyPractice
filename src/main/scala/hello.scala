import org.apache.spark.SparkContext

object hello {
  def main(args:Array[String]):Unit=
    {
//     val sc=new SparkContext("local[*]","razzaq")
//      val input=sc.textFile("C:/Users/DellX/Desktop/ForPractice/data.txt")
//         val rdd1=input.flatMap(x=>x.split(" "))
//         val rdd2=rdd1.map(x=>(x,1))
//         val rdd3=rdd2.reduceByKey((x,y)=>x+y)
//         val rdd4=rdd3.sortBy(x=>x._2,ascending = false)
//         rdd4.take(2).foreach(println)

//      val sc=new SparkContext("local[*]", "top20IPAddresses")
//      val input=sc.textFile("C:/Users/DellX/Desktop/ForPractice/ipaddresses.txt")
//
//      val rdd1=input.flatMap(x=>x.split(" "))
//      val rdd2=rdd1.map(x=>(x,1))
//      val rdd3=rdd2.reduceByKey((x,y)=>x+y)
//      val rdd4=rdd3.sortBy(x=>x._1,ascending = true)
//      rdd4.take(2).foreach(println)

      // convert array data -- into dataframe/rdd

      val sc=new SparkContext("local[*]","Average")
//      val arr=Array(10,20,30,40,50,60,70,80,91)
//
//      val rdd1=sc.parallelize(arr)
//
//     // val avg=rdd1.mean()
//
//      val sum=rdd1.reduce((x,y)=>x+y)
//      val c=rdd1.count()
//
//      val avg=sum/c
//
//      print(avg)

//      val rdd=sc.parallelize(Array(1,2,3,4,5))
//      rdd.saveAsTextFile("C:/Users/DellX/Desktop/ForPractice/avg")

//      val rdd1 = sc.parallelize(Array((1,"Apple"), (2, "Banana"), (3,"Orange")))
//
//      val rdd2 = sc.parallelize(Array((1,"Red"), (2,"Yellow"), (4,"Green")))
//
////      val joinedRdd = rdd1.leftOuterJoin(rdd2)
//      val joinedRdd = rdd1.rightOuterJoin(rdd2)
//      joinedRdd.foreach(println)


//      Subtracting

//      val rdd1 = sc.parallelize(Array(1,2,3,4,5))
//      val rdd2 = sc.parallelize(Array(3,4,5))
//
//      val rdd3=rdd2.subtract(rdd1)
//
//      rdd3.collect.foreach(println)

//      Intersection

//      val rdd1 = sc.parallelize(Array(1,2,3,4,5))
//      val rdd2 = sc.parallelize(Array(3,4,5))
//
//      val rdd3=rdd2.intersection(rdd1)
//
//      rdd3.collect.foreach(println)

//      Union

//      val rdd1 = sc.parallelize(Array(1,2,3,4,5))
//      val rdd2 = sc.parallelize(Array(3,4,5))
//
//      val rdd3=rdd2.union(rdd1)
//
//      rdd3.collect.foreach(println)

//     Cartesian

//      val rdd1 = sc.parallelize(Array(1,2,3,4,5))
//      val rdd2 = sc.parallelize(Array("A","B","C"))
//
//      val rdd3=rdd1.cartesian(rdd2)
//
//      rdd3.collect.foreach(println)

//      Conditional
//      val rdd1 = sc.parallelize(Array(1,2,3,4,5))
//      val rdd2 = rdd1.filter(x=>x%2==0)
//
//      rdd2.collect.foreach(println)

//      Copy the news article in to the text file and find a word ex: murder

//      val rdd1 = sc.parallelize(Array(1,2,3,4,5))
//      val search=4
//
//      val rdd2=rdd1.filter(x=>x==search)
//
//      rdd2.collect.foreach(println)

//      val rdd1 = sc.parallelize(Array("apple","banana","carrot"))
//
//      val search="carr"
////      Search does not work on subset of a word
//      val rdd2=rdd1.filter(x=>x==search)
//
//      rdd2.collect.foreach(println)

//      val rdd1 = sc.parallelize(Array("apple","banana","carrot"))
//
//      val search="carr"
//      //      To search subset of a word use contains method
//      val rdd2=rdd1.filter(x=>x.contains(search))
//
//      rdd2.collect.foreach(println)

//      Consider there are 100 words and converting back to word,1. Use map partitions



//      scala.io.StdIn.readLine()

    }

}
