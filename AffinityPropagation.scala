package main

import scala.collection.mutable
import org.apache.spark.{SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * param id: l'id del cluster che corrisponde all'id dell'exemplar
 * param exemplar: il centro, exemplar, del cluster
 * param member: l'id del membro del cluster
 */
@Experimental
case class AffinityPropagationAssignment(val id: Int, val exemplar: Int, val member: Int)

/**
 * param id: l'id del cluster che corrisponde all'id dell'exemplar
 * param exemplar: il centro, exemplar, del cluster
 * param members: i membri appartenenti al cluster
 */
@Experimental
case class AffinityPropagationCluster(val id: Int, val exemplar: Int, val members: Array[Int])

/**
 * param assignments: le designazioni del cluster, ottenute come risultato dell'algoritmo dell'Affinity Propagation
 */
@Experimental
class AffinityPropagationModel(val assignments: RDD[AffinityPropagationAssignment]) extends Serializable {

  /**
   * Il numero dei clusters ottenuti dall'esecuzione dell'algoritmo di Affinity Propagation
   */
  lazy val k: Int = assignments.map(_.id).distinct.count().toInt
 
  /**
   * Restituisce il cluster a cui appartiene il vertice passato come parametro
   * param vertexID: l'id del vertice
   * retval: RDD contentete i vari id appertenenti allo stesso cluster del vertexID; se vertexID non appartiene a nessun cluster viene restituito null
   */
  def findCluster(vertexID: Int): RDD[Int] = {
    val assign = assignments.filter(_.member == vertexID).collect()
    if (assign.nonEmpty) {
      assignments.filter(_.id == assign(0).id).map(_.member)
    } else {
      assignments.sparkContext.emptyRDD[Int]
    }
  } 
 

  /**
   * Trova l'id del  cluster a cui appartiene il vertice passato come parametro
   * param vertexID: l'id del vertice
   * retval: l'id del cluster a cui appartiene il vertexID; se vertexID non appartiene a nessun cluster viene restituito -1
   */
  def findClusterID(vertexID: Int): Int = {
    val assign = assignments.filter(_.member == vertexID).collect()
    if (assign.nonEmpty) {
      assign(0).id
    } else {
      -1
    }
  } 

  /**
   * Converte gli assignment in rappresentazioni cluster
   * retval: RDD di AffinityPropagationCluster contenente i clusters generati dall'algoritmo di Affiity Propagation
   */
  def fromAssignToClusters(): RDD[AffinityPropagationCluster] = {
    assignments.map { assign => ((assign.id, assign.exemplar), assign.member) }
      .aggregateByKey(mutable.Set[Int]())(
        seqOp = (s, d) => s ++ mutable.Set(d),
        combOp = (s1, s2) => s1 ++ s2
      ).map(kv => new AffinityPropagationCluster(kv._1._1, kv._1._2, kv._2.toArray))
  }
}

/**
 * Il messaggio che viene scambiato tra i nodi del grafo
 */
case class EdgeMessage(similarity: Double, availability: Double, responsibility: Double) extends Equals {
  override def canEqual(that: Any): Boolean = {
    that match {
      case e: EdgeMessage =>
        similarity == e.similarity && availability == e.availability &&
          responsibility == e.responsibility
      case _ =>
        false
    }
  }
}

/**
 * L'attributo di ogni vertice del grafo
 */
case class VertexData(availability: Double, responsibility: Double)

/**
 * Affinity propagation (AP), svliluppato da Brendan J. Frey e Delbert Dueck
 * [http://www2.warwick.ac.uk/fac/sci/dcs/research/combi/seminars/freydueck_affinitypropagation_science2007.pdf]
 *
 * param maxIterations: il numero massimo di iterazioni dell'algoritmo Maximum number of iterations of the AP algorithm.
 * param lambda: parametro per convergenza matrice
 * param normalization: indica se va eseguita la normalizzazione sulla matrice di similarità Indication of performing normalization
 * param symmetric: indica se la matrice di similarità è simmetrica oppure no
 */
class AffinityPropagation (private var maxIterations: Int, private var lambda: Double, private var normalization: Boolean, private var symmetric: Boolean) extends Serializable {

  import main.AffinityPropagation._

  /** 
   * Instanza di Affinity Propagation con parametri di default
   */
  def this() = this(maxIterations = 100, lambda = 0.5, normalization = false, symmetric = true)

  /**
   * Imposta il numero massimo di iterazioni
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }
 
  /**
   * Restituisce il numero massimo di iterazioni dell'algoritmo
   */
  def getMaxIterations: Int = {
    this.maxIterations
  }
 
  /**
   * Imposta il valore lambda per l'algoritmo
   */
  def setLambda(lambda: Double): this.type = {
    this.lambda = lambda
    this
  }
 
  /**
   * Restituisce il valore lambda fissato per l'algoritmo
   */
  def getLambda(): Double = {
    this.lambda
  }
 
  /**
   * Imposta se va effettuata la normalizzazione sulla matrice di similarità oppure no
   */
  def setNormalization(normalization: Boolean): this.type = {
    this.normalization = normalization
    this
  }
 
  /**
   * Restituisce un valore booleano che indica se la matrice di similarità è stata normalizzata oppure no
   */
  def getNormalization(): Boolean = {
    this.normalization
  }

  /**
   * Imposta se la matrice di similaritò è simmetrica oppure no.
   * Se symmetric è uguale a true allora si assume che la matrice ricevuta in input è una matrice triangolare
   */
  def setSymmetric(symmetric: Boolean): this.type = {
    this.symmetric = symmetric
    this
  }

  /**
   * Restituisce un valore booleano che indica se la matrice di similarità è simmetrica oppure no
   */
  def getSymmetric(): Boolean = {
    this.symmetric
  }

  /**
   * Calcola il valore mediano delle similarità
   */
  private def getMedian(similarities: RDD[(Int, Int, Double)]): Double = {
    val sorted: RDD[(Int, Double)] = similarities.sortBy(_._3).zipWithIndex().map {
      case (v, idx) => (idx.toInt, v._3)
    }.persist(StorageLevel.MEMORY_AND_DISK)

    val count = sorted.count().toInt

    val median: Double =
      if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (sorted.lookup(l).head + sorted.lookup(r).head).toDouble / 2
      } else {
        sorted.lookup(count / 2).head
      }
    sorted.unpersist()
    median
  }

  /**
   * Funzione che costruisce il grafo partendo dalla matrice di similarità ed esegue poi l'algoritmo di Affinity Propagation
   *
   * param similarities: RDD di tuple del tipo ( i, j, s(i,j) ) che rappresenta la matrice di similarità, la matrice S indicata nel paper.
   *                     Le tuple in cui i = j vengono indicate come "preferences"
   * retval: AffinityPropagationModel contenente il risultato dell'esecuzione dell'algoritmo
   */
  def createAPModel(similarities: RDD[(Int, Int, Double)]) : AffinityPropagationModel = {
    val s = constructGraph(similarities, normalization, this.symmetric)
    runAP(s)
  }

 
  /**
   * Runs the AP algorithm.
   *
   * param s: il grafo ottenuto dalla matrice di similarità
   * retval: AffinityPropagationModel contenente il risultato dell'esecuzione dell'algoritmo
   */
  private def runAP(s: Graph[VertexData, EdgeMessage]): AffinityPropagationModel = {
    val g = apIteration(s, maxIterations, lambda)
    chooseExemplars(g)
  }
}

object AffinityPropagation {
  /**
   * Costruisce la matrice di similarità S, effettuando la normalizzazione se richiesto. Restituisce S
   */
  def constructGraph(similarities: RDD[(Int, Int, Double)], normalize: Boolean, symmetric: Boolean): Graph[VertexData, EdgeMessage] = {

    val edges = similarities.flatMap { case (i, j, s) =>
      if (symmetric && i != j) {
        Seq(Edge(i, j, new EdgeMessage(s, 0.0, 0.0)), Edge(j, i, new EdgeMessage(s, 0.0, 0.0)))
      } else {
        Seq(Edge(i, j, new EdgeMessage(s, 0.0, 0.0)))
      }
    }

    var g = Graph.fromEdges(edges, new VertexData(0.0, 0.0))
    if (normalize) {
      val gA = Graph.fromEdges(edges, 0.0)
      val vD = gA.aggregateMessages[Double](
        sendMsg = ctx => {
          ctx.sendToSrc(ctx.attr.similarity)
        },
        mergeMsg = (s1, s2) => s1 + s2,
        TripletFields.EdgeOnly)
      val normalized = GraphImpl.fromExistingRDDs(vD, gA.edges)
        .mapTriplets({ e =>
            val s = if (e.srcAttr == 0.0) e.attr.similarity else e.attr.similarity / e.srcAttr
            new EdgeMessage(s, 0.0, 0.0)
        }, TripletFields.Src)
      g = Graph.fromEdges(normalized.edges, new VertexData(0.0, 0.0))
    } else {
      g = Graph.fromEdges(edges, new VertexData(0.0, 0.0))
    }

    g
	
  }

  /**
   * Esecuzione dell'iterazione dell'algoritmo di Affinity Propagation
   * param g: grafo ottenuto dalla matrice di similarità con i collegamenti che hanno come attributo gli EdgeMessage, ovvero la tupla <similarity, availability, responsibility>
   * param maxIterations: il numero masssimo di iterazioni dell'algoritmo
   * retval: il grafo finale con i centroid ottenuti dopo gli scambi di messaggi
   */
  def apIteration(g: Graph[VertexData, EdgeMessage], maxIterations: Int, lambda: Double): Graph[VertexData, EdgeMessage] = {
    val tol = math.max(1e-5 / g.vertices.count(), 1e-8)
    var prevDelta = (Double.MaxValue, Double.MaxValue)
    var diffDelta = (Double.MaxValue, Double.MaxValue)
    var curG = g
    var it = 0
    var convergence = false

    for (iter <- 0 until maxIterations if(!convergence) ){
      it = iter
      if (math.abs(diffDelta._1) > tol || math.abs(diffDelta._2) > tol) {
	      println("iter: " + iter)

	      // aggiornamento responsibilities
	      val vD_r = curG.aggregateMessages[Seq[Double]](
		sendMsg = ctx => ctx.sendToSrc(Seq(ctx.attr.similarity + ctx.attr.availability)),
		mergeMsg = _ ++ _,
		TripletFields.EdgeOnly)

	      val updated_r = GraphImpl(vD_r, curG.edges)
		.mapTriplets({ e =>
		  val filtered = e.srcAttr.filter(_ != (e.attr.similarity + e.attr.availability))
		  val pool = if (filtered.size < e.srcAttr.size - 1) {
		    filtered :+ (e.attr.similarity + e.attr.availability)
		  } else {
		    filtered
		  }
		  val maxValue = if (pool.isEmpty) 0.0 else pool.max
		  new EdgeMessage(e.attr.similarity,
		    e.attr.availability,
		    lambda * (e.attr.similarity - maxValue) + (1.0 - lambda) * e.attr.responsibility)
		}, TripletFields.Src)


	      var iterG = Graph.fromEdges(updated_r.edges, new VertexData(0.0, 0.0))
		
	      // aggiornamento availabilities
	      val vD_a = iterG.aggregateMessages[Double](
		sendMsg = ctx => {
		  if (ctx.srcId != ctx.dstId) {
		    ctx.sendToDst(math.max(ctx.attr.responsibility, 0.0))
		  } else {
		    ctx.sendToDst(ctx.attr.responsibility)
		  }
		}, mergeMsg = (s1, s2) => s1 + s2,
		TripletFields.EdgeOnly)

	      val updated_a = GraphImpl(vD_a, iterG.edges)
		.mapTriplets(
		  (e) => {
		    if (e.srcId != e.dstId) {
		      val newA = lambda * math.min(0.0, e.dstAttr - math.max(e.attr.responsibility, 0.0)) +
		                 (1.0 - lambda) * e.attr.availability
		      new EdgeMessage(e.attr.similarity, newA, e.attr.responsibility)
		    } else {
		      val newA = lambda * (e.dstAttr - e.attr.responsibility) +
		        (1.0 - lambda) * e.attr.availability
		      new EdgeMessage(e.attr.similarity, newA, e.attr.responsibility)
		    }
		  }, TripletFields.Dst)


	      iterG = Graph.fromEdges(updated_a.edges, new VertexData(0.0, 0.0))


	     // Dopo un certo numero di iterazioni viene esguito il checkpointing sul grafo così da "troncare il
	      curG = iterG
		if(iter > 0 && (iter+1) % (maxIterations / 4) == 0){
			curG.vertices.checkpoint
			curG.edges.checkpoint
			println("it checkpoint nV:" + curG.vertices.count + ", nE: " + 
			curG.edges.count)
			println("debugV: " + curG.vertices.toDebugString)
			println("debugE: " + curG.edges.toDebugString)
			}

	      }
     else convergence = true
    }

    println("\n\n---------------------------------------------------\nN iterazioni: " + (it+1) +  "\n---------------------------------------------------")

    curG
  }
 

  /**
   * Choose exemplars for nodes in graph.  Funzione per scegliere i centroid, exemplars, per i nodi del grafo finale
   * param g: grafo i cui collegameti contengono come attributo i valori finali per le availabilities e responsibilities
   * retval: AffinityPropagationModel con i risultati dell'algoritmo di clustering
   */
  def chooseExemplars(g: Graph[VertexData, EdgeMessage]): AffinityPropagationModel = {
    println("\n\n---------------------------------------------------\nchooseExemplars\n---------------------------------------------------")
	val accum = g.edges.map(a => (a.srcId, (a.dstId, a.attr.availability + a.attr.responsibility))).reduceByKey((ar1, ar2) => {
	      if (ar1._2 >= ar2._2) (ar1._1, ar1._2)
	      else (ar2._1, ar2._2)
	    })
	//accum.checkpoint
//println("n accum " + accum.count)
    println("aggregate")
	 val clusterMembers = accum.map(kv => (kv._2._1, kv._1)).aggregateByKey(mutable.Set[Int]())(
	      seqOp = (s, d) => s ++ mutable.Set(d.toInt),
	      combOp = (s1, s2) => s1 ++ s2
	    ).persist()


    /*println("zipWithIndex")
	val cm = clusterMembers.zipWithIndex()*/
    
    println("assignments")
	    val assignments = clusterMembers.repartition(32).flatMap { kv =>
	      kv._2.map(new AffinityPropagationAssignment(kv._1.toInt, kv._1.toInt, _))
	    }
println("part: " + assignments.partitions.size)
assignments.persist()
println("persist")

//println(assignments.collect.size)

assignments.collect.foreach(x => println(x))
	
	    new AffinityPropagationModel(assignments)
  }
}
