package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object TriangleCount2 {

  /**
  * Efficently enumerates all triangles in the graph. See the docs for explanations of the algorithm
  * @param graph Input graph, treated as an undirected graph
  * @tparam T Type of the node
  * @tparam E Type of the edge weight(ordered)
  */

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) = {
    
      // remove multi edges
      val g = graph.groupEdges((a, b) => a).cache()
      val degree: VertexRDD[Int] = g.degrees

      // order nodes lexically
      val edges = g.edges.map{ v => 
        if (v.srcId <= v.dstId)
           ((v.srcId,v.dstId),None)
        else
          ((v.dstId,v.srcId),None)
      }

      // append the degree of each node to it
      val degreeGraph = g.outerJoinVertices(degree) { (id, oldAttr, degreeOpt) =>
          degreeOpt match {
            case Some(outDeg) => outDeg
            case None => 0
          }
      }


      val triangles = degreeGraph
      .triplets
      .map {triplet =>
        if ((triplet.srcAttr <= triplet.dstAttr) && (triplet.srcId < triplet.dstAttr))
          (triplet.srcId,triplet.dstId)        
        else         
          (triplet.dstId,triplet.srcId)
      }
      .groupByKey      
      .flatMap { v =>
      
        val neighborsList = collection.mutable.ArrayBuffer[VertexId]()      
        val iter = v._2.iterator
        while (iter.hasNext) {
          val t = iter.next()           
          neighborsList += t
        }
        
        for (x <- neighborsList; y <- neighborsList if (x < y)) 
          yield ((x,y), v._1)                      
      }
      .join(edges)
      .map( v => (v._1._1,v._1._2,v._2._1) )      

      triangles          

  }

    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Spark TraingleCount2Test")
      val sc = new SparkContext(conf)
      val rawEdges = sc.parallelize(Array( 0L->1L, 0L->2L, 3L->1L, 3L->2L, 3L->4L, 1L->2L), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = TriangleCount2.run(graph).count        
      println(triangleCount)
  }


}
