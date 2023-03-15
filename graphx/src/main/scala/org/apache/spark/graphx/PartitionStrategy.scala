/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

/**
 * Represents the way edges are assigned to edge partitions based on their source and destination
 * vertex IDs.
 */
trait PartitionStrategy extends Serializable with Logging {
  /** Returns the partition number for a given edge. */
  def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID

  def getPartitionedEdgeRDD[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                        numPartitions: PartitionID, degreeCutoff: Int): RDD[(PartitionID, (VertexId, VertexId, ED))]
}

/**
 * Represents Default Partition Strategy
 *
 */
trait DefaultPartitionStrategy extends PartitionStrategy {
  override def getPartitionedEdgeRDD[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                                 numPartitions: PartitionID, degreeCutoff: Int): RDD[(PartitionID, (VertexId, VertexId, ED))] = {
    graph.edges.map { e =>
      val part: PartitionID = getPartition(e.srcId, e.dstId, numPartitions)
      (part, (e.srcId, e.dstId, e.attr))
    }
  }

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    // Some random implementation
    val mixingPrime: VertexId = 1125899906842597L
    (math.abs((src + dst) * mixingPrime) % numParts).toInt
  }
}

/**
 * Collection of built-in [[PartitionStrategy]] implementations.
 */
object PartitionStrategy {
  /**
   * Assigns edges to partitions using a 2D partitioning of the sparse edge adjacency matrix,
   * guaranteeing a `2 * sqrt(numParts)` bound on vertex replication.
   *
   * Suppose we have a graph with 12 vertices that we want to partition
   * over 9 machines.  We can use the following sparse matrix representation:
   *
   * <pre>
   * __________________________________
   * v0   | P0 *     | P1       | P2    *  |
   * v1   |  ****    |  *       |          |
   * v2   |  ******* |      **  |  ****    |
   * v3   |  *****   |  *  *    |       *  |
   * ----------------------------------
   * v4   | P3 *     | P4 ***   | P5 **  * |
   * v5   |  *  *    |  *       |          |
   * v6   |       *  |      **  |  ****    |
   * v7   |  * * *   |  *  *    |       *  |
   * ----------------------------------
   * v8   | P6   *   | P7    *  | P8  *   *|
   * v9   |     *    |  *    *  |          |
   * v10  |       *  |      **  |  *  *    |
   * v11  | * <-E    |  ***     |       ** |
   * ----------------------------------
   * </pre>
   *
   * The edge denoted by `E` connects `v11` with `v1` and is assigned to processor `P6`. To get the
   * processor number we divide the matrix into `sqrt(numParts)` by `sqrt(numParts)` blocks. Notice
   * that edges adjacent to `v11` can only be in the first column of blocks `(P0, P3,
   * P6)` or the last
   * row of blocks `(P6, P7, P8)`.  As a consequence we can guarantee that `v11` will need to be
   * replicated to at most `2 * sqrt(numParts)` machines.
   *
   * Notice that `P0` has many edges and as a consequence this partitioning would lead to poor work
   * balance.  To improve balance we first multiply each vertex id by a large prime to shuffle the
   * vertex locations.
   *
   * When the number of partitions requested is not a perfect square we use a slightly different
   * method where the last column can have a different number of rows than the others while still
   * maintaining the same size per block.
   */
  case object EdgePartition2D extends DefaultPartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
      val mixingPrime: VertexId = 1125899906842597L
      if (numParts == ceilSqrtNumParts * ceilSqrtNumParts) {
        // Use old method for perfect squared to ensure we get same results
        val col: PartitionID = (math.abs(src * mixingPrime) % ceilSqrtNumParts).toInt
        val row: PartitionID = (math.abs(dst * mixingPrime) % ceilSqrtNumParts).toInt
        (col * ceilSqrtNumParts + row) % numParts

      } else {
        // Otherwise use new method
        val cols = ceilSqrtNumParts
        val rows = (numParts + cols - 1) / cols
        val lastColRows = numParts - rows * (cols - 1)
        val col = (math.abs(src * mixingPrime) % numParts / rows).toInt
        val row = (math.abs(dst * mixingPrime) % (if (col < cols - 1) rows else lastColRows)).toInt
        col * rows + row

      }
    }
  }

  /**
   * Assigns edges to partitions using only the source vertex ID, colocating edges with the same
   * source.
   */
  case object EdgePartition1D extends DefaultPartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val mixingPrime: VertexId = 1125899906842597L
      val partitionId = (math.abs(src * mixingPrime) % numParts).toInt
      partitionId
    }
  }

  case object CustomPartition extends DefaultPartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val partitionId = (src / 1000000).toInt
      partitionId
    }
  }


  /**
   * Assigns edges to partitions by hashing the source and destination vertex IDs, resulting in a
   * random vertex cut that colocates all same-direction edges between two vertices.
   */
  case object RandomVertexCut extends DefaultPartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      math.abs((src, dst).hashCode()) % numParts
    }
  }


  /**
   * Assigns edges to partitions by hashing the source and destination vertex IDs in a canonical
   * direction, resulting in a random vertex cut that colocates all edges between two vertices,
   * regardless of direction.
   */
  case object CanonicalRandomVertexCut extends DefaultPartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      if (src < dst) {
        math.abs((src, dst).hashCode()) % numParts
      } else {
        math.abs((dst, src).hashCode()) % numParts
      }
    }
  }

  case object EdgePartition1DByDst extends DefaultPartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val mixingPrime: VertexId = 1125899906842597L
      (math.abs(dst * mixingPrime) % numParts).toInt
    }
  }

  case object HybridSrc extends DefaultPartitionStrategy {
    override def getPartitionedEdgeRDD[VD: ClassTag, ED: ClassTag](
                                                                    graph: Graph[VD, ED], numPartitions: PartitionID, degreeCutoff: Int): RDD[(
      PartitionID, (VertexId, VertexId, ED))] = {
      val out_degrees = graph.edges.map(e => (e.srcId, (e.dstId, e.attr))).
        join(graph.outDegrees.map(e => (e._1, e._2)))
      out_degrees.map { e =>
        var part: PartitionID = 0
        val srcId = e._1
        val dstId = e._2._1._1
        val attr = e._2._1._2
        val Degree = e._2._2
        val mixingPrime: VertexId = 1125899906842597L
        if (Degree > degreeCutoff) {
          part = ((math.abs(dstId) * mixingPrime) % numPartitions).toInt
        } else {
          part = ((math.abs(srcId) * mixingPrime) % numPartitions).toInt
        }
        (part, (srcId, dstId, attr))
      }
    }
  }

  case object HybridDst extends DefaultPartitionStrategy {
    override def getPartitionedEdgeRDD[VD: ClassTag, ED: ClassTag](
                                                                    graph: Graph[VD, ED], numPartitions: PartitionID, degreeCutoff: Int): RDD[(
      PartitionID, (VertexId, VertexId, ED))] = {
      val in_degrees = graph.edges.map(e => (e.dstId, (e.srcId, e.attr))).
        join(graph.inDegrees.map(e => (e._1, e._2)))
      in_degrees.map { e =>
        var part: PartitionID = 0
        val srcId = e._2._1._1
        val dstId = e._1
        val attr = e._2._1._2
        val Degree = e._2._2
        val mixingPrime: VertexId = 1125899906842597L
        if (Degree > degreeCutoff) {
          part = ((math.abs(srcId) * mixingPrime) % numPartitions).toInt
        } else {
          part = ((math.abs(dstId) * mixingPrime) % numPartitions).toInt
        }
        (part, (srcId, dstId, attr))
      }
    }
  }

  case object GreedySrc extends DefaultPartitionStrategy {
    override def getPartitionedEdgeRDD[VD: ClassTag, ED: ClassTag](
                                                                    graph: Graph[VD, ED], numPartitions: PartitionID, degreeCutoff: Int): RDD[(
      PartitionID, (VertexId, VertexId, ED))] = {
      val mixingPrime: VertexId = 1125899906842597L

      // Count the number of edges corresponding to each srcID.
      val groupedSrc = graph.edges.map { e => (e.srcId, e.dstId) }.groupByKey

      // Assuming initial random partitioning based on dstId
      // Count the overlap for each SrcId with each Partition.
      val srcEdgeCount = groupedSrc.map { e =>
        var srcOverlap = new Array[Long](numPartitions)
        e._2.map { dsts => srcOverlap((math.abs(dsts * mixingPrime) % numPartitions).toInt) += 1 }
        (e._1, srcOverlap)
      }

      // An array to capture the load on each partition
      // as we greedily assign edges to different partitions
      var current_load = new Array[Long](numPartitions)

      val FinalSrcAssignment = srcEdgeCount.map { e =>
        val src = e._1
        val dst = e._2

        // Randomly assign the src id to a partition.
        var part: PartitionID = (math.abs(src * mixingPrime) % numPartitions).toInt

        // Go over each partition and see with which partitions the neighbors of this vertex
        // overlap the most. Also take into account that the partition doesn't get too heavy.
        var mostOverlap: Double = dst.apply(part) - math.sqrt(1.0 * current_load(part))
        for (cur <- 0 to numPartitions - 1) {
          val overlap: Double = dst.apply(cur) - math.sqrt(1.0 * current_load(cur))
          if (overlap > mostOverlap) {
            part = cur
            mostOverlap = overlap
          }
        }

        // All the edges associated with this source vertex is sent to partition choosen
        // Hence we increase the edge count of this partition in the current load.
        for (cur <- 0 to numPartitions - 1) {
          current_load(part) += dst.apply(cur)
        }
        (src, part)
      }

      // Join the found partition for each source to its edges.
      val PartitionedRDD = (graph.edges.map { e => (e.srcId, (e.dstId, e.attr)) }).
        join(FinalSrcAssignment.map { e => (e._1, e._2) })
      PartitionedRDD.map { e =>
        (e._2._2, (e._1, e._2._1._1, e._2._1._2))
      }
    }
  }

  case object GreedyDst extends DefaultPartitionStrategy {
    override def getPartitionedEdgeRDD[VD: ClassTag, ED: ClassTag](
                                                                    graph: Graph[VD, ED], numPartitions: PartitionID, degreeCutoff: Int): RDD[(
      PartitionID, (VertexId, VertexId, ED))] = {
      val mixingPrime: VertexId = 1125899906842597L

      // Count the number of edges corresponding to each dstID.
      val groupedDst = graph.edges.map { e => (e.dstId, e.srcId) }.groupByKey

      // Assuming initial random partitioning based on srcId
      // Count the overlap for each DstId with each Partition.
      val dstEdgeCount = groupedDst.map { e =>
        var dstOverlap = new Array[Long](numPartitions)
        e._2.map { srcs => dstOverlap((math.abs(srcs * mixingPrime) % numPartitions).toInt) += 1 }
        (e._1, dstOverlap)
      }

      // An array to capture the load on each partition
      // as we greedily assign edges to different partitions
      var current_load = new Array[Long](numPartitions)

      val FinalDstAssignment = dstEdgeCount.map { e =>
        val dst = e._1
        val src = e._2

        // Randomly assign the destination id to a partition.
        var part: PartitionID = (math.abs(dst * mixingPrime) % numPartitions).toInt

        // Go over each partition and see with which partitions the neighbors of this vertex
        // overlap the most. Also take into account that the partition doesn't get too heavy.
        var mostOverlap: Double = src.apply(part) - math.sqrt(1.0 * current_load(part))
        for (cur <- 0 to numPartitions - 1) {
          val overlap: Double = src.apply(cur) - math.sqrt(1.0 * current_load(cur))
          if (overlap > mostOverlap) {
            part = cur
            mostOverlap = overlap
          }
        }

        // All the edges associated with this destination vertex is sent to partition choosen
        // Hence we increase the edge count of this partition in the current load.
        for (cur <- 0 to numPartitions - 1) {
          current_load(part) += src.apply(cur)
        }
        (dst, part)
      }

      // Join the found partition for each destination to its edges.
      val PartitionedRDD = (graph.edges.map { e => (e.dstId, (e.srcId, e.attr)) }).
        join(FinalDstAssignment.map { e => (e._1, e._2) })
      PartitionedRDD.map { e =>
        (e._2._2, (e._2._1._1, e._1, e._2._1._2))
      }
    }
  }


  /** Returns the PartitionStrategy with the specified name. */
  def fromString(s: String): PartitionStrategy = s match {
    case "RandomVertexCut" => RandomVertexCut
    case "CustomPartition" => CustomPartition
    case "EdgePartition1D" => EdgePartition1D
    case "EdgePartition1DByDst" => EdgePartition1DByDst
    case "EdgePartition2D" => EdgePartition2D
    case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
    case "HybridSrc" => HybridSrc
    case "HybridDst" => HybridDst
    case "GreedySrc" => GreedySrc
    case "GreedtDst" => GreedyDst
    case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
  }
}
