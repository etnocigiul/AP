import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Dict{

	private val MAXINT = 100000

	def main(args: Array[String]) {
		
		val inputFile = "/home/bijo/Prove/Dict/sentences.txt"
		val conf = new SparkConf().setAppName("Dict")
		val sc = new SparkContext(conf)

		val input = sc.textFile(inputFile)
		val dict = input.flatMap(line => line.split("[^\\w]+")).filter(v => v.length > 1).map(x => (x, 1)).reduceByKey((x, y) => x + y).sortByKey()

		val numbers = dict.map(v => v._2.toDouble).filter(x => x > 1).distinct()

		val rm = recursiveMap(numbers)D

		println("|||||||||||||||||||||||||||||||||||||||||||||||\n|||||||||||||||||||||||||||||||||||||||||||||||")
		dict.foreach(v => println(v._1 + ": \t\t" + v._2))
		println("|||||||||||||||||||||||||||||||||||||||||||||||\n|||||||||||||||||||||||||||||||||||||||||||||||")
		numbers.foreach(println(_))
		println("|||||||||||||||||||||||||||||||||||||||||||||||\n|||||||||||||||||||||||||||||||||||||||||||||||")
		rm.foreach(println(_))
		println("|||||||||||||||||||||||||||||||||||||||||||||||\n|||||||||||||||||||||||||||||||||||||||||||||||")
		

		val max = numbers.max
		dict.filter( _._2 == max).foreach(v => println(v._1))
		println("|||||||||||||||||||||||||||||||||||||||||||||||\n|||||||||||||||||||||||||||||||||||||||||||||||")

		}

	def recursiveMap(num : RDD[Double]) : RDD[Double] = if(num.max < MAXINT) recursiveMap(num.map(x => x*x)) else num

}
