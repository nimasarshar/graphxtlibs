package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object RectangleCount2 {


  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) = {
    
    // Remove redundant edges 
      val g = graph.groupEdges((a, b) => a).cache()
  
      val degree: VertexRDD[Int] = g.degrees

      val edges = g.edges.map{ v => 
        if (v.srcId <= v.dstId)
           ((v.srcId,v.dstId),None)
        else
          ((v.dstId,v.srcId),None)
      }

      /* append the degree of each node as its attribute */

      val degreeGraph = g.outerJoinVertices(degree) { (id, oldAttr, degreeOpt) =>
          degreeOpt match {
            case Some(outDeg) => outDeg
            case None => 0
          }
      }

      val openApex = degreeGraph
      .triplets
      // pass on the degree of each node
      // we first bin on both nodes (regardless of which one has higher degree)
      .flatMap {triplet =>        
          Seq (((triplet.srcId, triplet.srcAttr) , (triplet.dstId,triplet.dstAttr)),   
            ((triplet.dstId,triplet.dstAttr) , (triplet.srcId, triplet.srcAttr)))                  
      }
      .groupByKey      
      /* collect all neighors */
      .flatMap { case ((v,degree),neighbors) =>      
        val neighborsList = collection.mutable.ArrayBuffer[(VertexId,Int)]()      
        val iter = neighbors.iterator
        while (iter.hasNext) {
          val t = iter.next()           
          neighborsList += t
        }

        /* for each pair of neighbors create and open apex ONLY if the degree of 
        * at least one of the neighbors is 
        */
        for (x <- neighborsList; y <- neighborsList if ((x._1 < y._1) && (degree <= x._2 || degree <= y._2))) 
          yield ((x._1,y._1), v)                                        
      }

      

      /* now join the open apex RDD on it's own along the open apex */

      val closedApex = openApex
      .join(openApex)
      .map {apex => 
        if (apex._2._1!=apex._2._2)  // every apex gets mapped to itslef once, so ignore that
        {
          val sortedNodes : Seq[Long] = Seq(apex._1._1,apex._1._2,apex._2._1,apex._2._2)
          val sorted=sortedNodes.sorted
          sorted
        }
      }
      .filter( _ != ())
      /* note that the same rectanlge will appear twice, so you need to do distinct */
      .distinct      
      closedApex
  }

    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Spark RectangleCount2Test")
      val sc = new SparkContext(conf)
      val rawEdges = sc.parallelize(Array( 0L->1L, 0L->2L, 3L->1L, 3L->2L, 3L->4L, 1L->2L), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val rectangleCount = RectangleCount2.run(graph).count
      println(rectangleCount)

  }
  


}