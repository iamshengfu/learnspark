package shengfu

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object test extends App {

  var arr = Array(13,5,5,1,9,20,40,32,3,16,25,13)
  //quickSortNonRecursive(arr)
  //selectionSort(arr)
  mergesort(arr,0,arr.length-1)
  println(arr.mkString(" "))

  def mergesort(arr: Array[Int],left : Int, right:Int): Int = {
    if(right <= left){
      return 1
    }
    if(right - left == 1){
      if(arr(left)>arr(right)){
        var tmp = arr(left)
        arr(left) = arr(right)
        arr(right) = tmp
      }
      return 1
    }
    var mid = (right + left)/2
    mergesort(arr,left,mid - 1)
    mergesort(arr,mid,right)
    merge(arr,left,mid,right)
    return 1
  }

  def merge(arr:Array[Int],left:Int,mid:Int,right:Int) = {
    var i = left
    var j = mid
    var k = 0
    var tmp = Array.ofDim[Int](right-left+1)
    while(i < mid && j <= right){
      if(arr(i)<arr(j)){
        tmp(k) = arr(i)
        i += 1
        k += 1
      }else {
        tmp(k) = arr(j)
        j += 1
        k += 1
      }
    }
    while(i < mid){
      tmp(k) = arr(i)
      i += 1
      k += 1
    }
    while(j<=right){
      tmp(k) = arr(j)
      j += 1
      k += 1
    }
    for(i <- 0 to tmp.length-1){
      arr(left+i) = tmp(i)
    }
  }

  def selectionSort(arr:Array[Int]) = {
    for(i <- 1 to arr.length - 1){
      var key = arr(i)
      var j = i - 1
      breakable{
        while (j>=0){
          if(arr(j)>key){
            arr(j+1) = arr(j)
            j -= 1
          }else{
            break
          }
        }
      }
      arr(j+1) = key
    }
  }

  def quickSort(arr:Array[Int], left:Int, right:Int):Unit = {
    if(left < right){
      var i = rearrange(arr,left,right)
      quickSort(arr, left, i-1)
      quickSort(arr,i+1, right)
    }
  }

  def quickSortNonRecursive(arr:Array[Int]) = {
    val todos = new ArrayBuffer[Array[Int]]()
    todos.append(Array(0,arr.length - 1))
    while(todos.length > 0){
      var one = todos.last
      todos.trimEnd(1)
      var i = rearrange(arr,one(0),one(1))
      var a = Array(one(0),i-1)
      var b = Array(i+1,one(1))
      if(i-1 > one(0)){
        todos += a
      }
      if(one(1) > i+1){
        todos += b
      }
    }
  }

  def rearrange(arr:Array[Int], left:Int, right:Int): Int = {
    val peg = arr(left)
    var i = left
    var j = right
    while(j >= i){
      while(arr(i)<=peg & j >= i){
        i += 1
      }
      while(arr(j)>peg & j >= i){
        j -= 1
      }
      if(i<=j){
        swap(arr,i,j)
      }else{
        swap(arr,left,j)
      }
    }
    return j
  }
  def swap(arr:Array[Int], i:Int, j:Int) = {
    var tmp = arr(i)
    arr(i) = arr(j)
    arr(j) = tmp
  }
}
