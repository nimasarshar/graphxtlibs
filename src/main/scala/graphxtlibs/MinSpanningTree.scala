package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.util.Random
import scala.collection.mutable._


object MinSpanningTree {

  /**
  * An implementation of the Prime's algorithm to compute all edges that belong to a MST in one connected component
  * It goes through all connected components and does not assume that the graph is connected
  * @param edges An iterator of edges in the form (v_1,(v_1,e_12)) v_1->v_2 is the edge with weight e_12
  * @tparam T Type of the node
  * @tparam E Type of the edge weight(ordered)
  */
	class LocalMSTAlgorithm[T,E <% Ordered[E]](edges : Iterator[(T,(T,E))])
	{

	  var totalEdges=0
	  val empty  = new HashMap[T,ArrayBuffer[(T,E)]]()
	  def addEdge(n1 : T,n2 : T, w: E, adj : HashMap[T,ArrayBuffer[(T,E)]])
	  {
	    adj.get(n1) match {
	       case Some(x) => 
	          x += ((n2,w))
	       case None => 
	          val et= ArrayBuffer[(T,E)]((n2,w))
	          adj += (n1 -> et)
	     }
	  }

	  val adjList = edges.foldLeft(empty){
	    (adj,t) => 
	     val (n1,(n2,w)) = t
	     if (n1!=n2) // in case of a self-loop just add the link once
	     {
	         addEdge(n2,n1,w,adj)
	         addEdge(n1,n2,w,adj)
	     }
	     totalEdges += 1
	     adj
	  }

	  val visited = new HashSet[T]()
	  val touchedEdges = PriorityQueue[(T,T,E)]()(Ordering.by((_: (T,T,E))._3).reverse)
	  val mst = new HashSet[(T,T,E)]()
	  val allMSTEdges = new HashSet[(T,T,E)]()

	  def visit(node : T)
	  {
	    adjList.get(node) match {
	      case Some(x) =>
	        visited += node
	        for (n <- x) touchedEdges += ((node,n._1,n._2))
	    }
  	  }
  
	  def allMSTs() =
	  {
	      var label = 0
	      while (adjList.size>0)
	      {

	        val node=adjList.head._1
	        visit(node)
	        while (touchedEdges.size>0)
	        {
	          val e = touchedEdges.dequeue
	          if (!visited.contains(e._1) || !visited.contains(e._2))
	          {
	            mst += e
	            allMSTEdges += e
	          }
	          if (e._1!=e._2 && !visited.contains(e._1)) 
	             visit(e._1)
	          if (e._1!=e._2 && !visited.contains(e._2)) 
	            visit(e._2)
	       	  
	        }

	        // now remove all the edges in the current component
	        for (e <- mst)
	        {          
	          adjList -= e._1
	          adjList -= e._2
	        }
	        touchedEdges.clear
	        mst.clear        
	        label += 1

	      }
	      
	      allMSTEdges
	  	}

	  }


	  def run[VD: ClassTag](graph: Graph[VD, Double], maxEdgesOnDriver : Long, numNodes: Long) = {
		  var liveRDD =graph
	        .triplets
	        .map(t => (t.srcId,(t.dstId,t.attr)))
	      var edgesLeft= liveRDD.count
	      while (edgesLeft>maxEdgesOnDriver) 
	      {
	      	var numPartitions = edgesLeft/numNodes	      	
	        liveRDD = liveRDD.repartition(numPartitions.toInt)       
	        liveRDD=liveRDD.mapPartitions{ edgeIter =>	        	           
	              val mstCalculator = new LocalMSTAlgorithm[VertexId,Double](edgeIter)            
	              mstCalculator.allMSTs.iterator.map(v=> (v._1,(v._2,v._3)))
	        }
	        edgesLeft = liveRDD.count
	      }
	      val edgesOnDriver =liveRDD.collect()
	      val mstCalculatorOnDriver = new LocalMSTAlgorithm[VertexId,Double](edgesOnDriver.iterator)
	      mstCalculatorOnDriver.allMSTs
	  }


	  def main(args: Array[String]) {

	      val conf = new SparkConf().setAppName("Spark Local Page Rank2")
	      val sc = new SparkContext(conf)	      
	      val graph = GraphGenerators.starGraph(sc,10).mapEdges(_ => 1.0)		  
		  val numNodes = graph.vertices.count 
		  val densityFactor=1.2
	      MinSpanningTree.run(graph,(numNodes*densityFactor).toLong,numNodes)
	                
	  }

}
