import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Dict{

	private val MAXINT = 100000

	def main(args: Array[String]) {
		
		val inputFile = "/home/bijo/Prove/Dict/sentences.txt"

		//Inizializzo lo SparkContext
		val conf = new SparkConf().setAppName("Dict")
		val sc = new SparkContext(conf)

		/*
		Carico i dati che mi servono per la creazione del dizionario
		Il dizionario sarà una Map composta da elemeti del tipo <Parola, n° occorrenze>
		*/
		val input = sc.textFile(inputFile)
		val dict = input.flatMap(line => line.split("[^\\w]+")).filter(v => v.length > 1).map(x => (x, 1)).reduceByKey((x, y) => x + y).sortByKey()

		//Ricavo una lista di Double dai valori distinti del dizionario
		val numbers = dict.map(v => v._2.toDouble).filter(x => x > 1).distinct()

		val rm = recursiveMap(numbers)


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

	/* 
	Funzione map ricorsiva
	param num : RDD di Double a cui va applicato il quadrato per ogni elemento

	Se il valore massimo dell'RDD supera il limite stabilito non viene eseguita nessuna map, altrimenti si esegue la funzione sull'RDD a cui viene applicata la map
	*/
	def recursiveMap(num : RDD[Double]) : RDD[Double] = if(num.max < MAXINT) recursiveMap(num.map(x => x*x)) else num

}
