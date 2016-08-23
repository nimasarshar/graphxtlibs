package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.util.Random
import scala.collection.mutable

  /**
  * A random walk
  * @param headNode The node that the walk started at
  */

class Walk (val headNode:VertexId) extends Serializable
{
  val id:Long = Random.nextLong  
  var walkNodes = Seq[VertexId]()  
  def :+(v: VertexId)
  {
    walkNodes = walkNodes :+ v
  }
  def tailNode : VertexId = walkNodes.last    
}

object RandomWalk {

  val rnd: Random = new Random

  private def  pickRandom(nodes: Array[VertexId]) : VertexId =
  {    
    
    nodes(rnd.nextInt(nodes.size))
    
  }

  private def advanceWalk[ED: ClassTag](graph: Graph[(mutable.ArrayBuffer[Walk], Array[VertexId]),ED]) = 
  {
        
    val walkAhead=graph.vertices
       .flatMap { v =>     
        
        var s = Seq[(VertexId,Walk)]()                 
        for (w <- v._2._1)
        {        
          val t= v._2._2 :+ v._1 // add the node itself to make the walk recurrent          
          val n= pickRandom(t)        
          w :+ n 
          s = s :+ (n,w)
        }       
        s
      }
      .groupByKey()
                
      graph.mapVertices((id, attr) => (mutable.ArrayBuffer[Walk](), attr._2))
        .joinVertices(walkAhead){
          (id, w, allWalks) =>         
            for (walk <- allWalks)      
              w._1 += walk             
            w
      }                  
  }

  private def bringWalksBackHome[ED: ClassTag](graph: Graph[(mutable.ArrayBuffer[Walk], Array[VertexId]),ED]) : Graph[(mutable.ArrayBuffer[Walk], Array[VertexId]),ED] =
  {    
    val v1=graph.vertices
    .flatMap{ v =>
      for (w <- v._2._1) yield (w.headNode,w)              
    }
    .groupByKey()
    
    val graphQ = graph.mapVertices((id, attr) => (mutable.ArrayBuffer[Walk](), attr._2))          
    val res= graphQ.joinVertices(v1){
        (id, w, allWalks) =>         
        for (walk <- allWalks)      
          w._1 += walk             
        w
      }      
    res     
  }

  private def initWalkGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED] , numSeeds: Int) =
  {
        val g = graph.groupEdges((a, b) => a).cache()
        val neighbors=g.collectNeighborIds(EdgeDirection.Out)
        var st=g.outerJoinVertices(neighbors){
            (id,v,neighbors) =>  
            val nb=neighbors match {
              case Some(neighbors) => neighbors
              case None => Array[VertexId]()
            }          
            val initialWalkSet = mutable.ArrayBuffer[Walk]()
            for ( i <- 0 until numSeeds)
            {
              val w: Walk = new Walk(id)          
              initialWalkSet += w
            }
            
            (initialWalkSet,nb)
        }
        st

  }

  /**
  * Performs multiple random walks starting from all nodes in the graph. 
  * @param graph The input graph
  * @param shortWalkLength The length of each random walk
  * @param numSeeds The number of walks initiated from each graph
  * @tparam T Type of the node
  * @tparam E Type of the edge weight(ordered)
  */

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], shortWalkLength: Int, numSeeds: Int, mapWalksBack2HeadNode: Boolean = true) = {      
      var st = initWalkGraph(graph, numSeeds)            
      for (i <- 0 until shortWalkLength) st = advanceWalk(st) 
      if (mapWalksBack2HeadNode)   
        bringWalksBackHome(st)
      else 
        st
  }
  
}