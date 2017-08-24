package main
 
import java.lang.Exception
import scala.collection.mutable
import scala.math
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import main.AffinityPropagation._



object APExecutor extends Serializable{


	def main(args: Array[String]) {
		

		val startTime = System.nanoTime()
		val nIt = 100

		val ip = "192.168.x.x"
		val port = "9010"
		val dirInput = "path/to/test/dir/"
		val dirEdges = "aut_N/"
		val dirSim = "sim_M/"

		val inputPath = "hdfs://" + ip + ":" + port + "/user/name/"
		//Inizializzo lo SparkContext
		try{		
			val conf = new SparkConf().setAppName("APExecutor")
			val sc = new SparkContext(conf)
		
		sc.setCheckpointDir(inputPath + "checkpoint")
		
		//Dall'input ricavo prima i collegamenti tra i vari autori, e poi da questi ottengo una lista dove ad ogni autore associo la lista degli altri autori con i quali ha collaborato
		val input = sc.textFile(inputPath + dirInput + dirSim + "part-00000.gz")//.repartition(8)
		val vertices = sc.textFile(inputPath + dirInput + dirEdges + "part-00000.gz")//.repartition(8)
		

		//Creo una matrice di similarità vuota, in cui ogni coppia di vertici ha come valore nullo come similarità
		val empty_matrix: RDD[((Int, Int), Double)] = vertices.cartesian(vertices).map(x => ((x._1.toInt, x._2.toInt), 0.0) )
		
		//Ricavo dal file di input la matrice di similarità sparsa, ovvero la matrice non contenente valori uguali a zero
		val sparse_matrix: RDD[((Int, Int), Double)]= input.map{l => val a = l.split("\t"); ((a(0).toInt, a(1).toInt), a(2).toDouble)}
		
		//Dall'unione della matrice vuota e di quella sparsa ottengo la matrice densa di similarità, quindi la matrice contenente tutti i valori compresi quelli uguali a zero. Poiché questa matrice è già simmetrica all'algoritmo va passato come parametro symmetric = false per evitare errori
		val similarities_matrix = sparse_matrix.union(empty_matrix).reduceByKey(_+_).map{x => 
if (x._2 == 0.0) (x._1._1, x._1._2, -100.0) else (x._1._1, x._1._2, x._2)}.repartition(32)
		
		similarities_matrix.persist()
		similarities_matrix.checkpoint()



		println("N sim: " + similarities_matrix.count())
		println("debug: " + similarities_matrix.toDebugString)
		//Ottengo un affinity propagation model a cui passare le similarities ottenute per poi ottenere i vari clusters
val elapsedTimeStart = System.nanoTime() - startTime
		println("\n-----------------------------------------------------\nTrascorsi " + (elapsedTimeStart / 60000000000.0) + " minuti\n-----------------------------------------------------")

val startTimeAP = System.nanoTime()
println("\t\t-------> Inizio AP")
		val affinityPropagation = new AffinityPropagation()
		val apModel = affinityPropagation.setMaxIterations(nIt).setSymmetric(false).createAPModel(similarities_matrix)
println("\t\t-------> Fine AP")

		val elapsedTimeAP = System.nanoTime() - startTimeAP

		println("\n-----------------------------------------------------\nTempo di esecuzione Affinity Propagation: " + (elapsedTimeAP / 60000000000.0) + " minuti\n-----------------------------------------------------")
		//Ottengo un RDD contenente i cluster risultanti dall'esecuzione dell'algoritmo di Affinity Propagation
 		val clusters = apModel.fromAssignToClusters

		//Applico una trasformazione alla matrice di similarità per ottenere i soli valori di similarità
		//val similarityValues = similarities_matrix.map(x => x._3)

		//Salvo le informazioni che mi interessano su file
		val string = "-----------------------------------\n\n\nnum cluster: " + apModel.k + ", preference: " + similarities_matrix.filter(x => x._1 == x._2).map(x => x._3).min + ", max S: " + similarityValues.max + ", min S: " + similarityValues.min + ", nIter: " + nIt + "\n-----------------------------------"
		
		println(string)

		//Viene eseguita una map sull'RDD dei cluster per rendere leggibile il risultato, viene poi salvato su file
		val outFile = clusters.map(v =>"idCluster: " + v.id + ", exemplar/vertice: " + v.exemplar + ", clusterMembers --> " + v.members.toList)
outFile.foreach(x => println(x))
		outFile.coalesce(1).saveAsTextFile("hdfs://" + ip + ":" + port + "/user/conte/output/test2.clusters_" + apModel.k)
		



		val elapsedTime = System.nanoTime() - startTime
		println("\n-----------------------------------------------------\nTempo di esecuzione: " + (elapsedTime / 60000000000.0) + " minuti\n-----------------------------------------------------")

	}
		catch{
			case e: Exception => println("Eccezione: " + e)
		}
	}
}
