package com.shu.spark

import scala.collection.mutable.ArrayBuffer

object func_scala {
  def main(args: Array[String]): Unit = {
    /*
 		* intersect will result all the characters in both strings
 		*/
    println("hello".intersect("collection"))
    println("hello" intersect ("collection"))
    println("dadaddav".distinct)
    println("dadaddav".head)
    println("dadaddav".size)
    println("dadaddav".reverse)
    println("AaLl".filter(x => x.isUpper))
    println("AaLl".filter(x => x.isLower))
    /*
     * Array buffer
     */
    val ar_buf = ArrayBuffer[Int]()
    ar_buf += 9
    ar_buf += (1, 2, 3, 4)
    ar_buf ++= Array(66, 76)
    println(ar_buf)
    ar_buf.trimEnd(2) //trim 2 elements at the end
    println(ar_buf)
    ar_buf.insert(2, 2, 6, 5) //insert into array buffer from 2 index
    println(ar_buf)
    ar_buf.remove(2, 2) //remove from 2 index
    println(ar_buf)
    val to_arr = ar_buf.toArray
    println(to_arr.toBuffer)

    val list = new java.util.ArrayList[String]()
    list.add("abc")
    println(list.toArray())
    val map = new java.util.HashMap[String, Int] //key,value pairs
    map.put("a", 10)
    println(map)

    //collections
    val lis = List(1, 2, 3)
    println(lis.isEmpty)
    println(lis == Nil)
    println(lis.sorted.reverse)
    val ll = List(-1, -10, 34, 22, 22, 1)
    println(ll.sorted.reverse)
    println(ll.sortWith((x, y) => x + "" < y + "")) // -1+"" is smaller than -2+""
    println(Set(3, 1, 1, 2))
    println(List(1, 1, 2))
    println(Set(1, 2, 3) == Set(3, 2, 1)) //true order doesn't matter
    println(List(1, 2, 3) == List(3, 2, 1)) //false order matter n list

    //map
    val map_kv = Map("lang" -> "fp", "name" -> "scala")
    println(if (map_kv.contains("lang")) map_kv("lang") else "not there")
    println(map_kv.getOrElse("kk", "not there")) //shorthand for above syntax
    println(map_kv.get("kk").getOrElse("not there"))
    println(map_kv.get("lang"))
    for ((k, v) <- map_kv) println(v, k)
    println(map_kv.keySet) //print only the keys

    //streams

    val stream = (1 to 100).toStream
    println(stream)
    println(stream.filter(_ % 10 == 0))
    println(stream.filter(_ % 10 == 0).toList) //here we are storing into list
    val t = (10, "scl", 90.98)
    println(t._1) //access tuple elements
    val (first, second, third) = t
    println(first)

    /*
     * zipping
     */
    val sym = Array("*", "-", "*")
    val lck = Array(5, 10, 5)
    val pair = sym.zip(lck)

    for ((k, v) <- pair) print(k * v)
    val ke = List(1, 2, 3)
    val va = List("a", "b", "c")
    val zz = (ke zip (va)).toMap
    println(zz)
    zz.foreach(println)
    /*
 		 * assign only required values to variables
  		*/
    val t1 = (1, 3.14, "j1")
    val (first1, second1, _) = t1 //just assigns first (_1),second (_2) values and ignore rest.
    println("mew", second1)

    /*
     * Lists
     */
    //different forms of defining lists.
    val l1 = List(1, 2, 3)
    val l2 = Traversable(1, 2, 3)
    val l3 = Iterable(1, 2, 3)
    val l4 = Seq(1, 2, 3)
    val l5= "a"::"b"::"c"::Nil
    println(l1, l2, l3, l4, l5)
    //concat lists
    val l_odd = List(1, 3, 5, 7)
    val l_even = List(2, 4, 6)
    val l_num = l_odd ++ l_even
    println(l_num)
    val l_digit = 0 :: l_num
    println(l_digit)
    println(l_digit.contains(1))
    println(l_digit.tail)
    println(l_digit.last)
    println(l_digit.lastIndexOf(4))
    println(l_digit.mkString("**"))
    println(l_digit.size)
    println(l_digit.count(x => x*x >2)) //for each element multiply with it (x*x) and do count only if the x*x >2
    
  }
}