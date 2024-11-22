// Databricks notebook source
// MAGIC %md
// MAGIC # Hello World!

// COMMAND ----------

println("Hello World")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Variables

// COMMAND ----------

var x= 10

// COMMAND ----------

x = 20

// COMMAND ----------

val y = 5

// COMMAND ----------

y = 10

// COMMAND ----------

// MAGIC %md
// MAGIC We get an error because y is immutable.

// COMMAND ----------

// MAGIC %md
// MAGIC These two are equivalent:

// COMMAND ----------

val y: Int = 10;
val y = 10

// COMMAND ----------

// MAGIC %md
// MAGIC ## Functions

// COMMAND ----------

def add(firstInput: Int, secondInput: Int): Int = {
  val sum = firstInput + secondInput
  return sum
}

// COMMAND ----------

val addNumbers = add(20, 5)

// COMMAND ----------

def addSimple(firstInput: Int, secondInput: Int) = firstInput + secondInput

// COMMAND ----------

val addSimpleNumbers = addSimple(30,6)

// COMMAND ----------

// MAGIC %md
// MAGIC The difference between the two functions is that the second one is an expression.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Higher-Order Functions

// COMMAND ----------

def encode(n: Int, f: (Int) => Long): Long = {
  val x = n * 10
  f(x)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Function Literals

// COMMAND ----------

// MAGIC %md
// MAGIC An example:

// COMMAND ----------

(x : Int) => {
  x + 100
}

// COMMAND ----------

val higherOrderFunctionTest1 = encode(10, (x: Int) => {x + 100})

// COMMAND ----------

// MAGIC %md
// MAGIC Equivalent:

// COMMAND ----------

val higherOrderFunctionTest2 = encode(10, (x: Int) => x + 100)

// COMMAND ----------

// MAGIC %md
// MAGIC Equivalent:

// COMMAND ----------

val higherOrderFunctionTest3 = encode(10, x => x + 100)

// COMMAND ----------

// MAGIC %md
// MAGIC Equivalent:

// COMMAND ----------

val higherOrderFunctionTest4 = encode(10, _ + 100)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Classes

// COMMAND ----------

class Car(mk: String, ml: String, cr: String){
  val make = mk
  val model = ml
  var color = cr
  def repaint(newColor: String) = {
    color = newColor
  }
}

// COMMAND ----------

val mustang = new Car("Ford", "Mustang", "Red")
val corvette = new Car("GM", "Corvette", "Black")

// COMMAND ----------

corvette.repaint("Blue")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Singleton

// COMMAND ----------

// MAGIC %md
// MAGIC ### Case Class

// COMMAND ----------

case class Message(from: String, to: String, content: String)
// equivalent to
// class Message(val from: String, val to: String, val content: String)

// COMMAND ----------

val request = Message("harry", "sam", "discussion")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Pattern Matching

// COMMAND ----------

def colorToNumber(color: String): Int = {
  val num = color match {
    case "Red" => 1
    case "Blue" => 2
    case "Green" => 3
    case "Yellow" => 4
    case _ => 0
  }
  num
}

// COMMAND ----------

val colorName = "Red"
val colorCode = colorToNumber(colorName)
println(s"The color code for $colorName is $colorCode")

// COMMAND ----------

def f(x: Int, y: Int, operator: String): Double = {
  operator match {
    case "+" => x + y
    case "-" => x - y
    case "*" => x * y
    case "/" => x / y.toDouble
  }
}

// COMMAND ----------

val sum = f(10,20, "+")
val product = f(10, 20, "*")
