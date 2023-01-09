package miniLab.tablas

import miniLab.tablas
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.DataFrame

object CargaTablas {

  case class Personas(id_Persona: Int, genero: String, edad: Int, altura: Int, peso: Int)
  case class Familias(primos: Int, hermanos: Int, sobrinos: Int)
  case class Vehiculos(n_Serie: Int, tipo_Vehiculo: String, n_Ruedas: Int, matricula: String, combustible: String)
  case class Domicilios(persona: Int, pais: String, provincia: String, ciudad: String, c_Postal: String, puerta: String)
  case class Pertenencias(persona: Int, vehiculo: Int, casa: Byte, ordenador: Byte)

  // Mostrar solo errores en consola
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[4]")
    .appName("cargaTablas")
    .getOrCreate()
  import spark.implicits._
  // Estructura y carga de tablas

  // TODO VEHICULOS
  val vehiculosSchema = StructType(Array(
    StructField("n_Serie", IntegerType, true),
    StructField("tipo_Vehiculo", StringType, true),
    StructField("n_Ruedas", IntegerType, true),
    StructField("matricula", StringType, true),
    StructField("combustible", StringType, true)
  ))
  val vehiculosDataSet = spark.read
    .option("sep", ";")
    .option("charset", "ISO-8859-1")
    .schema(vehiculosSchema)
    .csv("C:/Developer/spark-scala-examples/src/main/scala/miniLab/data/Vehiculos.csv")
    .as[Vehiculos]
  //vehiculosDataSet.show()

  // TODO PERSONAS
  val personaSchema = StructType(Array(
    StructField("id_Persona", IntegerType, true),
    StructField("nombre", StringType, true),
    StructField("genero", StringType, true),
    StructField("edad", IntegerType, true),
    StructField("altura", IntegerType, true),
    StructField("peso", IntegerType, true),
    StructField("familiares", MapType(StringType, IntegerType, true), true)
  ))
  val structuredPersona = Seq(
    Row(1, "Marcos","M", 12, 198, 90, Map("primos" -> 0, "hermanos" -> 0, "sobrinos" -> 0)),
    Row(2, "Marc","M", 21, 198, 55, Map("primos" -> 8, "hermanos" -> 3, "sobrinos" -> 3)),
    Row(3, "Fran","M", 43, 198, 56, Map("primos" -> 8, "hermanos" -> 0, "sobrinos" -> 0)),
    Row(4, "Julepe","M", 76, 198, 54, Map("primos" -> 2, "hermanos" -> 4, "sobrinos" -> 0)),
    Row(5, "Sousa","M", 92, 134, 78, Map("primos" -> 1, "hermanos" -> 8, "sobrinos" -> 5)),
    Row(6, "Jorge","M", 23, 198, 87, Map("primos" -> 0, "hermanos" -> 5, "sobrinos" -> 0)),
    Row(7, "Maria","F", 54, 167, 43, Map("primos" -> 5, "hermanos" -> 8, "sobrinos" -> 7)),
    Row(8, "Maica","F", 45, 200, 105, Map("primos" -> 4, "hermanos" -> 6, "sobrinos" -> 5)),
    Row(9, "Micaela","F", 34, 190, 77, Map("primos" -> 1, "hermanos" -> 3, "sobrinos" -> 3)),
    Row(10,"Alba", "F", 34, 167, 89, Map("primos" -> 6, "hermanos" -> 9, "sobrinos" -> 3)),
    Row(11,"Arman", "M", 22, 198, 83, Map("primos" -> 3, "hermanos" -> 2, "sobrinos" -> 4)),
    Row(12,"Emmanuè", "M", 54, 186, 60, Map("primos" -> 7, "hermanos" -> 2, "sobrinos" -> 4)),
    Row(13,"Massi", "F", 23, 155, 55, Map("primos" -> 0, "hermanos" -> 8, "sobrinos" -> 3)),
    Row(14,"Malena", "F", 29, 179, 90, Map("primos" -> 0, "hermanos" -> 2, "sobrinos" -> 0)),
    Row(15,"Chute", "M", 32, 198, 90, Map("primos" -> 8, "hermanos" -> 2, "sobrinos" -> 1))

  )
  val personasDataSet = spark.createDataFrame(
    spark.sparkContext.parallelize(structuredPersona), personaSchema
  )

  // TODO DOMICILIOS
  val domiciliosSchema = StructType(Array(
    StructField("persona", IntegerType, true),
    StructField("pais", StringType, true),
    StructField("provincia", StringType, true),
    StructField("ciudad", StringType, true),
    StructField("c_Postal", StringType, true),
    StructField("puerta", StringType, true)
  ))
  val domiciliosDataSet = spark.read
    .option("sep", ";")
    .option("charset", "ISO-8859-1")
    .schema(domiciliosSchema)
    .csv("C:/Developer/spark-scala-examples/src/main/scala/miniLab/data/Domicilios.csv")
    .as[Domicilios]
  //domiciliosDataSet.toDF()
  //domiciliosDataSet.write.format("parquet").parquet("data/domicilios.parquet")

  // TODO PERTENENCIAS
  val pertenenciasSchema = StructType(Array(
    StructField("persona", IntegerType, true),
    StructField("vehiculo", IntegerType, true),
    StructField("casa", ByteType, true),
    StructField("ordenador", ByteType, true)
  ))
  val pertenenciasDataSet = spark.read
    .option("sep", ";")
    .option("charset", "ISO-8859-1")
    .schema(pertenenciasSchema)
    .csv("C:/Developer/spark-scala-examples/src/main/scala/miniLab/data/Pertenencias.csv")
    .as[Pertenencias]

  //pertenenciasDataSet.show()




}
