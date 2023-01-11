package miniLabScala

import miniLab.tablas.CargaTablas.{personasDataSet, pertenenciasDataSet, spark, vehiculosDataSet}
import org.apache.spark.sql.functions.{col, expr}

object TestCar {
  def main(args: Array[String]): Unit = {
    val datosPersonas = personasDataSet
      .join(pertenenciasDataSet, pertenenciasDataSet("persona") === personasDataSet("id_Persona"), "inner")
      .join(vehiculosDataSet, pertenenciasDataSet("vehiculo") === vehiculosDataSet("n_Serie"), "inner")
      .select("id_Persona", "nombre", "genero", "edad", "altura", "peso", "familiares", "casa", "ordenador", "tipo_Vehiculo", "matricula", "combustible")

    val parsedGenero = datosPersonas.select(col("nombre"), col("familiares"), col("tipo_vehiculo"),col("casa"),
      expr("CASE WHEN genero = 'M' THEN 'Hombre' " +
        "WHEN genero = 'F' THEN 'Mujer' " +
        "ELSE 'Otro Genero' END").alias("Genero"),
      expr("CASE " +
        "WHEN casa = 1 THEN CASE " +
        "WHEN ordenador = 1 THEN 'Con Casa y Ordenador' " +
        "WHEN ordenador = 0 THEN 'Con casa y Sin Ordenador' END" +
        "WHEN casa = 0 THEN CASE " +
        "WHEN ordenador = 1 THEN 'Sin Casa Pero Con Ordenador' " +
        "ELSE 'Sin Casa Ni Ordenador' END" +
        "END").alias("DomicilioPropio"))
    datosPersonas.show(false)
    parsedGenero.write.mode("overwrite").format("text").save("data/tablePersonas")
    val conexion : SqlOperations.Conexion = SqlOperations.conexionDefault.copy(url = "jdbc:mysql://localhost/mysql/Personas")
    var dbPersonas = SqlOperations.DatabaseData("DatosPersonas", "Personas", "data/tablePersonas.txt")
    SqlOperations.createDatabase(databaseName = dbPersonas.dbName, tableLocalURL = dbPersonas.tableURL)
    val gotPersonasTable = spark.read
      .format("jdbc")
      .option("driver", conexion.driver)
      .option("url", conexion.url)
      .option("dbtable", dbPersonas.tableName)
      .option("user", conexion.username)
      .option("password", conexion.password)
      .load()
      .cache()

    if (!gotPersonasTable.filter(s"SELECT ISNUMERIC(familiares['primos']) FROM ${dbPersonas.dbName}.${dbPersonas.tableName}").isEmpty) {
      val camposSelect = Array("id_Persona", "nombre", "genero", "edad", "casa", "tipo_Vehiculo", "matricula", "familires")
      val caseClause = s"WHEN familires['primos'] + familires['hermanos'] + familires['sobrinos'] > 7 THEN 'Numerosa' ELSE 'No Numerosa' END AS 'Familia'"
      val selectCaseExample = SqlOperations.consulta(camposSelect = camposSelect, tableName = dbPersonas.tableName, caseCoindition = caseClause)
      selectCaseExample.write.mode("overwrite").format("text").save("data/tablePersonasEdited")
      SqlOperations.createDatabase(databaseName = dbPersonas.dbName, tableName = dbPersonas.tableName, tableLocalURL = "data/tablePersonasEdited")
      SqlOperations.alterTable(tableName = dbPersonas.tableName, colName = "familiares", accion = "drop", conexion = conexion)
    }
    spark.stop()
  }
}
