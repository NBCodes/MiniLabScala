package miniLab

import miniLab.tablas.CargaTablas.{personasDataSet, pertenenciasDataSet, spark, vehiculosDataSet}
import org.apache.spark.sql.functions.{col, expr}

object TestCar {
  def main(args: Array[String]): Unit = {
    //CargaTablas.personasDataSet.show()
    //CargaTablas.personasDataSet.select(col("id_Persona"), col("familiares")).show()
    val datosPersonas = personasDataSet
      .join(pertenenciasDataSet, pertenenciasDataSet("persona") === personasDataSet("id_Persona"), "inner")
      .join(vehiculosDataSet, pertenenciasDataSet("vehiculo") === vehiculosDataSet("n_Serie"), "inner")
      .select("id_Persona", "nombre", "genero", "edad", "altura", "peso", "familiares", "casa", "ordenador", "tipo_Vehiculo", "matricula", "combustible")

    val parsedGenero = datosPersonas.select(col("nombre"), col("familiares"), col("tipo_vehiculo"),col("casa"),
      expr("CASE WHEN genero = 'M' THEN 'Hombre' " +
        "WHEN genero = 'F' THEN 'Mujer' " +
        "ELSE 'Otro Genero' END").alias("Genero")/*,
      expr("CASE " +
        "WHEN casa = 1 THEN CASE " +
        "WHEN ordenador = 1 THEN 'Con Casa y Ordenador' " +
        "WHEN ordenador = 0 THEN 'Con casa y Sin Ordenador' END" +
        "WHEN casa = 0 THEN CASE " +
        "WHEN ordenador = 1 THEN 'Sin Casa Pero Con Ordenador' " +
        "ELSE 'Sin Casa Ni Ordenador' END" +
        "END").alias("DomicilioPropio")*/)
    datosPersonas.show(false)
    // Gurda tabla modificada anteriormente
    parsedGenero.write.mode("overwrite").format("text").save("data/tablePersonas")
    // Se modifica La ruta de la URL para crear la tabla
    val conexion : SqlOperations.Conexion = SqlOperations.conexionDefault.copy(url = "jdbc:mysql://localhost/mysql/Personas")
    // llamada base de datos y creación tablas
    // Creo e instancio objeto para facilitar el usod de los datos mas adelante
    var dbPersonas = SqlOperations.DatabaseData("DatosPersonas", "Personas", "data/tablePersonas.txt")
    SqlOperations.createDatabase(databaseName = dbPersonas.dbName, tableLocalURL = dbPersonas.tableURL)

    // Pasar tabla creada anteriormente a un formato manejable con SparkSql para realizar Test
    val gotPersonasTable = spark.read
      .format("jdbc")
      .option("driver", conexion.driver)
      .option("url", conexion.url)
      .option("dbtable", dbPersonas.tableName)
      .option("user", conexion.username)
      .option("password", conexion.password)
      .load()
    // transformación de columna familiares
    if (!gotPersonasTable.filter(s"SELECT ISNUMERIC(familiares['primos']) FROM ${dbPersonas.dbName}.${dbPersonas.tableName}").isEmpty) {
      // Ejemplo de uso de SQL directamente en Base de datos MYSQL mediante función definida a código
      val camposSelect = Array("id_Persona", "nombre", "genero", "edad", "casa", "tipo_Vehiculo", "matricula", "familires")
      val caseClause = s"WHEN familires['primos'] + familires['hermanos'] + familires['sobrinos'] > 7 THEN 'Numerosa' ELSE 'No Numerosa' END AS 'Familia'"
      val selectCaseExample = SqlOperations.consulta(camposSelect = camposSelect, tableName = dbPersonas.tableName, caseCoindition = caseClause)
      selectCaseExample.write.mode("overwrite").format("text").save("data/tablePersonasEdited")
      // Se vuelve a crear la tablka a partir de la nueva modificación realizada
      SqlOperations.createDatabase(databaseName = dbPersonas.dbName, tableName = dbPersonas.tableName, tableLocalURL = "data/tablePersonasEdited")
      // Se elimina tabla modificada
      SqlOperations.alterTable(tableName = dbPersonas.tableName, colName = "familiares", accion = "drop", conexion = conexion)
    }
    spark.stop()
  }
}
