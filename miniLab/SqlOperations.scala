package miniLab

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.lang
import java.sql.{Connection, DriverManager, ResultSet}

object SqlOperations {
  case class Conexion (driver:String, url:String, username:String, password:String)
  case class DatabaseData (dbName: String, tableName: String, tableURL: String)
  // Conexion por defecto en la que se crean las tablas
  var conexionDefault = Conexion("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/mysql", "root", "root")


  val spark = SparkSession.builder
    .master("local[4]")
    .appName("querys")
    .getOrCreate()
  def consulta (camposSelect : Array[String] , tableName:String, whereCondition:String = "", caseCoindition:String, conexion: Conexion = conexionDefault): DataFrame ={
    // Crear conexion
    val connection:Connection = DriverManager.getConnection(conexion.url, conexion.username, conexion.password)
    Class.forName(conexion.driver)
    // Transforma array en formato correcto
    val campos = camposSelect.mkString(",")
    var queryResult: DataFrame = spark.emptyDataFrame

    try{
      // Consulta
      val stmt = connection.createStatement()
      val resultSet: ResultSet =
        if (caseCoindition.isEmpty) stmt.executeQuery(s"SELECT $campos FROM $tableName $whereCondition ;")
        else stmt.executeQuery(s"SELECT $campos CASE $caseCoindition FROM $tableName $whereCondition ;")
      // ReultSet a Dataframe
      //queryResult = spark.read.jdbc(resultSet)
    }catch{
      case e => e.printStackTrace
    }finally {
      connection.close()
    }
    queryResult
  }

  def alterTable (tableName: String, colName: String, accion:String, segAttr:String = "", conexion: Conexion = conexionDefault) ={
    // Crear conexion
    val connection: Connection = DriverManager.getConnection(conexion.url, conexion.username, conexion.password)
    Class.forName(conexion.driver)
    var queryResult: DataFrame = spark.emptyDataFrame
    try {
      val stmt = connection.createStatement()
      if (segAttr.isEmpty) stmt.executeQuery(s"ALTER TABLE  $tableName $accion COLUMN  $colName;")
      else if (accion.toLowerCase().contains("rename")) stmt.executeQuery(s"ALTER TABLE  $tableName $accion COLUMN  $colName TO $segAttr ;")
      else stmt.executeQuery(s"ALTER TABLE  $tableName $accion COLUMN  $colName $segAttr ;")

    }catch {
      case e => e.printStackTrace
    }finally {
      connection.close()
    }
  }
  // Creacion BaseDatos y Tablas a partir de rutas (.txt)
  def createDatabase(databaseName: String, tableName: String, tableLocalURL: String): Unit = {
    // Load the JDBC driver
    Class.forName("org.postgresql.Driver")

    // Establish a connection to the database
    val connection: Connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "user", "password")


    val stmtDropTable = connection.createStatement()
    val stmtDDBB = connection.createStatement()
    val stmtTable = connection.createStatement()
    try {
      stmtDropTable.executeUpdate(s"DROP TABLKE IF EXISTS $databaseName.$tableName")
      val database: Int = stmtDDBB.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $databaseName ;")
      val tablaPersonas = stmtTable.executeUpdate(s"CREATE TABLE IF NOT EXISTS $databaseName.$tableName " +
        s"( " +
        s"id_Persona Int," +
        s"nombre varchar," +
        s"genero varchar," +
        s"edad Int," +
        s"altura Int," +
        s"peso Int," +
        s"familiares MAPTYPE," +
        s"casa varchar," +
        s"tipo_vehiculo varchar," +
        s"matricula varchar," +
        s"combustible varchar," +
        s"domicilioPropio varchar" +
        s" )" +
        s"BULK INSERT $databaseName.$tableName " +
        s"FROM '$tableLocalURL' " +
        "WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\\u000a' )")
    }catch {
      case e => e.printStackTrace
    }finally {
      connection.close()
    }


  }

  spark.close()

}




