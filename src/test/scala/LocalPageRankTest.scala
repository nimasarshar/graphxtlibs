package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._




 class LocalPageRankTest extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf().setAppName("Spark LocalPageRank2Test").setMaster("local").set("spark.default.parallelism", "1")
  val sc = new SparkContext(conf)

  override def afterAll(): Unit ={
    this.sc.stop()
  }

  test("test_1") {

/*
                     0 <--
                      |___|             
*/                        

      val shortWalkLength = 2
      val numSeeds = 2
      val rawEdges = sc.parallelize(Array( 0L->0L), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val page = LocalPageRank.run(graph,shortWalkLength,numSeeds,stich=true)

      assert(page.count === 1)
      page.collect().foreach(v => assert(v._2==1.0))
  }
}
