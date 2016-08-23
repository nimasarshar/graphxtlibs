package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.util.GraphGenerators

import org.scalatest._


 class MinSpanningTreeTest extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf().setAppName("Spark MinSpanningTreeTest").setMaster("local").set("spark.default.parallelism", "1")
  val sc = new SparkContext(conf)

  override def afterAll(): Unit ={
    this.sc.stop()
  }

  test("test_1") {
      
      val graph = GraphGenerators.starGraph(sc,10).mapEdges(_ => 1.0)     
      val numNodes = graph.vertices.count 
      val densityFactor=1.2
      val tree = MinSpanningTree.run(graph,(numNodes*densityFactor).toLong,numNodes)
      assert(tree.size==9)      
      tree.foreach(v => assert(((v._1==0L || v._2==0)) && (v._3==1.0)))
  }
}
