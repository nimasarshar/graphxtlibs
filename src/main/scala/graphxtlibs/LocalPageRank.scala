package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.util.Random
import scala.collection.mutable


object LocalPageRank {

  /**
   # It stiches short random walks to construct a longer random walk before computing the approximation to the page rank
   * @param graph a utility graph. Each node contains numSeeds random walks of length shortWalkLength each, and the set of enighbors of the node
   * @tparam ED data associated to edges is ignored
   * @return a 3-tuple ((v1,v2), n_12) where v1,v2 are node indexes and n_12 is number of walks started from v1 that passes through v2
   */

  def stichWalks[ED: ClassTag](graph: Graph[(mutable.ArrayBuffer[Walk], Array[VertexId]),ED], shortWalkLength: Int) = 
  {
         
         var front=graph.vertices
         .flatMap { v =>
            for (i <- 0 until shortWalkLength)
              yield (v._1 , (v._1,i))   // (front, (base,iter))
         }

         var aggregator=graph.vertices
         .map { v =>
            ((v._1 , v._1),0)        // (base,neighbor,count)
         }

         for (i <- 0 until shortWalkLength)
         {
             val nextStepGraph=graph.vertices
             .join(front)

             aggregator = nextStepGraph.flatMap { v => 
                  val (frontId,((walks,neighbors),(baseId,iter))) = v
                  val w =walks(iter)
                  for (vid <- w.walkNodes) yield ((baseId,vid),1)                
             }
             .union(aggregator)
             .reduceByKey((a, b) => a + b)

             // now advane the front once
             front = nextStepGraph.map { v =>
                  val (frontId,((walks,neighbors),(baseId,iter))) = v
                  (walks(iter).tailNode,(baseId,iter))
             }
         }
         aggregator

  }

  /**
   # Computes the local (normalized page rank)
   * @param graph input graph
   * @param shortWalkLength The length of each individual walk
   * @param numSeeds The number of walks from each node: larger better appeoximation
   * @param stich Whether short walks should be stiched together to create longer walks.
   * @tparam VD data associated to nodes, is ignored
   * @tparam ED data associated to edges, is ignored
   * @return a 3-tuple ((v1,v2), p_12) where v1,v2 are node indexes and p_12 is approximation to local page rank from v1 to v2
   */
  
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED] , shortWalkLength: Int, numSeeds: Int, stich: Boolean = false) = {
        
    val st= RandomWalk.run(graph, shortWalkLength, numSeeds)
        
    val cnt=st.vertices.flatMap { v =>  
        val (vId,(walks,neighbors)) = v
        for (w <- walks; vid <- w.walkNodes) yield ((w.headNode,vid),1)
    }    
    .reduceByKey((a, b) => a + b)
    .map (v => (v._1, v._2/(shortWalkLength*numSeeds+0.0)))
    
    if (stich)
      stichWalks(st,shortWalkLength).map(v => (v._1, v._2/(shortWalkLength*numSeeds*numSeeds+0.0)))
    else    
      cnt      
  }
  
}