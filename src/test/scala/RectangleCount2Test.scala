package org.apache.spark.graphx.lib


import scala.reflect.ClassTag
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._




 class RectangleCount2Test extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf().setAppName("Spark RectangleCount2Test").setMaster("local").set("spark.default.parallelism", "1")
  val sc = new SparkContext(conf)

  override def afterAll(): Unit ={
    this.sc.stop()
  }

  test("test_1") {

/*
                 0
                / \
               1---2
                \ /
                 3 -- 4
*/              



      val rawEdges = sc.parallelize(Array( 0L->1L, 0L->2L, 3L->1L, 3L->2L, 3L->4L, 1L->2L), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val rectangleCount = RectangleCount2.run(graph).count
      assert(rectangleCount === 1)
  }
 }