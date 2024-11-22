// Databricks notebook source
// MAGIC %md
// MAGIC # Functions

// COMMAND ----------

// MAGIC %md
// MAGIC Refresher about functions

// COMMAND ----------

(x: Int) => x + 1

// COMMAND ----------

val addOne = (x: Int) => x + 1
println(addOne(1))

// COMMAND ----------

val add = (x: Int, y: Int) => x + y
println(add(1,2))

// COMMAND ----------

val getTheAnswer = () => 42
println(getTheAnswer()) // 42

// COMMAND ----------

// MAGIC %md
// MAGIC # Methods

// COMMAND ----------

// MAGIC %md
// MAGIC Methods are defined with def

// COMMAND ----------

def add(x: Int, y: Int): Int = x + y
println(add(1, 2))

// COMMAND ----------

def addThenMultiply(x: Int, y: Int)(multiplier: Int): Int = (x + y) * multiplier
println(addThenMultiply(1,2)(3))

// COMMAND ----------

// MAGIC %md
// MAGIC # Classes

// COMMAND ----------

class Greeter(prefix: String, suffix: String) {
  def greet(name: String): Unit =
    println(prefix + name + suffix)
}

// COMMAND ----------

val greeter = new Greeter("Hello, ", "!")
greeter.greet("Scala developer")

// COMMAND ----------

// MAGIC %md
// MAGIC # Case Classes

// COMMAND ----------

case class Point(x: Int, y: Int)

// COMMAND ----------

val point = Point(1,2)
val anotherPoint = Point(1,2)
val yetAnotherPoint = Point (2,2)

// COMMAND ----------

if (point == anotherPoint) {
  println(s"$point and $anotherPoint are the same")
} else {
  println("Not the same")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Objects

// COMMAND ----------

object IdFactory {
  private var counter = 0
  def create(): Int = {
    counter += 1
    counter
  }
}

// COMMAND ----------

val newId: Int = IdFactory.create()
println(newId)
val newerId: Int = IdFactory.create()
println(newerId)

// COMMAND ----------

// MAGIC %md
// MAGIC # Traits

// COMMAND ----------

trait Greeter {
  def greet(name: String): Unit
  = println("Hello, " + name + "!")
}

// COMMAND ----------

class DefaultGreeter extends Greeter

class CustomizableGreeter(prefix: String, postfix: String) extends Greeter {
  override def greet(name: String): Unit = {
    println(prefix + name + postfix)
  }
}

val greeter = new DefaultGreeter()
greeter.greet("Scala Developer")

val greeter2 = new CustomizableGreeter("Good Morning, ", "!")
greeter2.greet("Scala Developer")

// COMMAND ----------

object Main {
  def main(args: Array[String]): Unit =
    println("Hello, Scala Developer!")
}
