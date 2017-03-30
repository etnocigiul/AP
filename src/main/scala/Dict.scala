import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd._
 
object Dict {

	private val MAXINT = 5000000
	def main(args: Array[String]) {
		val inputFile = "/home/bijo/Prove/Dict/sentences.txt"

		//Inizializzo lo SparkContext
		val conf = new SparkConf().setAppName("Dict")
		val sc = new SparkContext(conf)

		//Carico l'input
		val input = sc.textFile(inputFile)

		//Creo un dizionario dall'input. In questa Map gli elementi sono del tipo (Parola, n°occorrenze)
		val dict = input.flatMap(line => line.split("\\P{L}+")).map(x => (x, 1)).reduceByKey((x, y) => x + y).sortByKey().partitionBy(new HashPartitioner(100)).persist()

		//Creo un RDD contenente solo i valori superiori a 1
		val numbers = dict.map(k => k._2.toDouble).filter(x => x > 1).distinct()

		val rm = recursiveMap(numbers)

		println("|||||||||||||||||||||||||||||||||||||||\n|||||||||||||||||||||||||||||||||||||||")
		dict.foreach(v => println(v._1 + ": \t" + v._2))
		println("|||||||||||||||||||||||||||||||||||||||\n|||||||||||||||||||||||||||||||||||||||")
		numbers.foreach(println(_))
		println("|||||||||||||||||||||||||||||||||||||||\n|||||||||||||||||||||||||||||||||||||||")
		println("Il max è: " + rm.max + ", superiore di " + (rm.max - MAXINT) + " rispetto a MAXINT")
		}

	// Funzione map ricorsiva
	def recursiveMap(num : RDD[Double]) : RDD[Double] = if(num.max < MAXINT) recursiveMap(num.map(x => x*x)) else num


}
