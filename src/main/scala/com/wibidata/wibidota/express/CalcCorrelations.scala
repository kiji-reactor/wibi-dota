package com.wibidata.wibidota.express

import org.kiji.express.{EntityId, AvroRecord, KijiSlice}
import com.wibidata.wibidota.express.DefaultResourceLocations._
import org.kiji.express.flow._
import com.twitter.scalding.{Csv, Mode, Args}
import com.twitter.scalding.mathematics.Matrix.pipeExtensions
import com.twitter.scalding
import cascading.pipe.Pipe
import cascading.pipe.joiner.OuterJoin
import com.twitter.scalding.mathematics.LiteralScalar

/**
 * Provides some methods to calculate the correlation coefficients
 * between some number of vectors. Vectors are inputted in sparse format
 * of (vectorId, index, value) tuples. vectorId and index are ints, values
 * are doubles. The fields should be called 'vec, 'index, 'val
 * The output will be (vectorId, vectorId, correlation) triples name
 * 'vec 'vec2 'cor.
 *
 * There are three methods to do this, matrixCorr, inMemCorr,
 * and mrCorr. inMemCorr and matrixCorr need to put vectors in memory
 * so configure your job accordingly.
 */
trait CalcCorrelations extends KijiJob {

  /*
   * Utility method to
   * calculate the correlation of two vectors given some statistics about them
   */
  private def correlation(size : Double, dotProduct : Double, sum1 : Double,
                          sum2 : Double, ss1 : Double, ss2 : Double) = {
    (size * dotProduct - sum1 * sum2) /
      (math.sqrt(size * ss1 - sum1 * sum1) * math.sqrt(size * ss2 - sum2 * sum2))
  }

  /**
   * Calculate the correlation using sclading's matrix class. Takes three jobs
   * and has to put entire rows in memory
   *
   * @param triples, data as specified in the class doc
   * @param vectorLen, length of the vectors
   * @return, the correlations
   */
  def matrixCorr(triples : Pipe, vectorLen : Int) : Pipe = {
    val matrix = triples.toMatrix[Int, Int, Double]('vec, 'index, 'val)
    // Normalize the vectors so we assume mean = 0 and std = 1 for all vectors
    val normalized = matrix.mapRows(normalize(vectorLen))
    (((normalized) * normalized.transpose) / new LiteralScalar(vectorLen.toDouble))
      .pipeAs('vec, 'vec2, 'val)
      .filter('vec, 'vec2){vecs : (Int, Int) => vecs._1 > vecs._2}
  }


  /*
   * Utility method used to normalize matrix rows, iterates through the row twice
   */
  private def normalize[T](count : Int)(vct: Iterable[(T,Double)]) : Iterable[(T,Double)] = {
    val vals = vct.map{ _._2 }
    val sum = vals.sum
    val avg = sum / count
    val std = math.sqrt(vals.map(x => math.pow(x - avg, 2)).sum / count)
    vct.map { tup => (tup._1, (tup._2 - avg) /std) }
  }


  /**
   * Calculates the correlation between vectors if we exclude columns for which
   * either matrix has a value of zero. 2 jobs.
   *
   * @param triples, data as specified in the class doc
   * @return, the correlations
   */
  // adapted from http://blog.echen.me/2012/02/09/movie-recommendations-and-more-via-mapreduce-and-scalding/
  def nonZeroCorrelations(triples : Pipe) : Pipe = {

    val triples2 = triples.rename(('vec, 'index, 'val) -> ('vec2, 'index2, 'val2))

    val pairs = triples.joinWithSmaller('index -> 'index2, triples2)
      .discard('index2)
      .filter('vec, 'vec2) { vecs: (Int, Int) => vecs._1 < vecs._2 }

    pairs.groupBy('vec, 'vec2){x =>
       x.foldLeft[Double, Double]('val -> 'valNormSq)(0.0){(a,b) => a + b * b}
        .foldLeft[Double, Double]('val2 -> 'val2NormSq)(0.0){(a,b) => a + b * b }
        .sum('val -> 'valSum)
        .sum('val2 -> 'val2Sum)
        .foldLeft[Double, (Double, Double)](('val, 'val2) -> 'prod)(0.0){(a,b) => a + b._1 * b._2}
        .size('size)
    }.map(('prod, 'valSum, 'val2Sum, 'valNormSq, 'val2NormSq, 'size) -> 'cor) {
      fields : (Double, Double, Double, Double, Double, Int) =>
        correlation(fields._6, fields._1, fields._2, fields._3, fields._4, fields._5)
    }.project('vec, 'vec2, 'cor)
  }


  /**
   * Calculates the correlation without putting an entire vector in memory. Takes
   * three jobs and a LOT of intermediate output. If the possible set of vector ids
   * is known beforehand they can be passed in, if not included we have to recalculate
   * them costing two more jobs.
   *
   * @param triples, data as specified in the class doc
   * @param vectorLen, length of the vectors
   * @param vectors, a sequence of the possible vector ids.
   * @return, the correlations
   */
  // TODO allows to us optionally guess the vector length rather then insist the user pass it in
  def correlations(triples : Pipe, vectorLen : Int, vectors : Option[Seq[Int]] = None) : Pipe = {

    val crossed : Pipe = vectors match {
      case Some(vecs) => {
        triples.flatMapTo(('vec, 'index, 'val) -> ('vec1, 'vec2, 'index, 'val)){
          tuple : (Int, Int, Double) =>  vecs.map(x => (x, tuple._1, tuple._2, tuple._3))
        }
      }
      case None => {
        val vecs = triples.project('vec).rename('vec -> 'vec2).unique('vec2)
        triples.crossWithTiny(vecs).rename('vec, 'vec1)
      }
    }


    val crossed2 = crossed.rename(('vec1, 'vec2, 'index, 'val) -> ('vec22,'vec12, 'index2, 'val2))
      .filter('vec12, 'vec22){vecs : (Int, Int) => vecs._1 > vecs._2}

    val p = crossed.filter('vec1, 'vec2){vecs : (Int, Int) => vecs._1 > vecs._2}
      .joinWithSmaller(('vec1, 'vec2, 'index) -> ('vec12, 'vec22, 'index2), crossed2, new OuterJoin)

    val pairs = p.mapTo(('index, 'val, 'vec1, 'vec2, 'index2, 'val2, 'vec12, 'vec22)
      -> ('index, 'val, 'val2, 'vec1, 'vec2)){x : (java.lang.Integer, Double, Int, Int,
      java.lang.Integer, Double, Int, Int) =>
      if (x._1 == null){
        (x._5, 0, x._6, x._7, x._8)
      } else if(x._5 == null) {
        (x._1, x._2, 0, x._3, x._4)
      } else {
        (x._1, x._2, x._6, x._3, x._4)
      }
    }

    pairs.groupBy('vec1, 'vec2){x =>
      x.foldLeft[Double, Double]('val -> 'valNormSq)(0.0){(a,b) => a + b * b}
        .foldLeft[Double, Double]('val2 -> 'val2NormSq)(0.0){(a,b) => a + b * b }
        .sum('val -> 'valSum)
        .sum('val2 -> 'val2Sum)
        .foldLeft[Double, (Double, Double)](('val, 'val2) -> 'prod)(0.0){(a,b) => a + b._1 * b._2}
    }.map(('prod, 'valSum, 'val2Sum, 'valNormSq, 'val2NormSq) -> 'cor) {
      fields : (Double, Double, Double, Double, Double) =>
        correlation(vectorLen, fields._1, fields._2, fields._3, fields._4, fields._5)
    }.project('vec1, 'vec2, 'cor)
  }

  /**
   * Calulates the correlation, but allowing whole vectors to be in memory.
   * How ever much more efficient than the other options if you can swing it.
   *
   * @param triples, data points to use
   * @param vectorLen, length of the vectors
   * @return, the correlations
   */
  def inMemCorrelations(triples : Pipe,  vectorLen : Int) : Pipe = {

    // Transform that data to lists per each column and precompute stats,
    // List are (index ,value) pairs sorted by index
    val cols = triples.groupBy('vec){gb => gb.toList[(Int, Int)]  (('index, 'val) -> 'valList)}
      .map('valList -> ('valSS, 'valS)){
      vl : List[(Int, Int)] => (vl.foldLeft(0.0)((a,b) => a + (b._2 * b._2)), vl.foldLeft(0.0)((a,b) => a + b._2))}
      .map('valList -> 'valList){
      f : List[(Int, Int)] => f.sortBy(x => x._1)
    }

    val cols2 = cols.rename(('vec, 'valList, 'valSS, 'valS) -> ('vec2, 'valList2, 'valSS2, 'valS2))

    // Cross with self to get the pairs
    cols.crossWithSmaller(cols2)
      // Filter out half the pairs to we do not recompute (A, A) or (A, B) and (B, A)
      // TODO maybe we can does this sooner to save work?
      .filter('vec, 'vec2){vecs : (Int, Int) => vecs._1 > vecs._2}

      // Get the dot product and know we have everything we need to find the correlation
      .map(('valList, 'valList2, 'vec, 'vec2) -> 'dotProduct){
      f : (List[(Int, Int)], List[(Int, Int)], Int, Int) =>
        sortedProduct(f._1, f._2)
    }.discard('valList, 'valList2)
      .map(('dotProduct, 'valS, 'valS2, 'valSS, 'valSS2) -> 'cor){
      f: (Double, Double, Double, Double, Double) =>
        correlation(vectorLen, f._1, f._2, f._3, f._4, f._5)
    }
      .project('vec, 'vec2, 'cor)
  }

  /*
   * Calculates the dot product of two sparse vectors, represented by (index, val) pairs
   * and sorted by index
   */
  private def sortedProduct(l1 : Seq[(Int, Int)], l2 : Seq[(Int, Int)]) : Double = {
    var product = 0.0;
    var index1 = 0
    var index2 = 0
    while(index1 < l1.size && index2 < l2.size){
      if(l1(index1)._1 == l2(index2)._1){
        product += l1(index1)._2 * l2(index2)._2.toDouble
        index1 += 1
        index2 += 1
      } else if(l1(index1)._1 > l2(index2)._1){
        index2 += 1
      } else {
        index1 += 1
      }
    }
    product
  }
}
