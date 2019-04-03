package com.shu.spark

import scala.collection.mutable.ArrayBuffer

object func_scala {
  def main(args: Array[String]): Unit = {
    /*
 		* intersect will result all the characters in both strings
 		*/
    println("hello".intersect("collection"))
    println("hello" intersect("collection"))
    println("dadaddav".distinct)
    println("dadaddav".head)
    println("dadaddav".size)
    println("dadaddav".reverse)
    println("AaLl".filter(x => x.isUpper))
    println("AaLl".filter(x => x.isLower))
    /*
     * Array buffer
     */
    val ar_buf=  ArrayBuffer[Int]()
    ar_buf +=9
    ar_buf +=(1,2,3,4)
    ar_buf ++=Array(66,76)
    println(ar_buf)
    ar_buf.trimEnd(2) //trim 2 elements at the end
    println(ar_buf)
    ar_buf.insert(2, 2,6,5) //insert into array buffer from 2 index
    println(ar_buf)
    ar_buf.remove(2,2) //remove from 2 index
    println(ar_buf)
    val to_arr=ar_buf.toArray
    println(to_arr.toBuffer)
  }
}