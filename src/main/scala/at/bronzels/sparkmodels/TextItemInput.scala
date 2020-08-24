package at.bronzels.sparkmodels

import scala.beans.BeanProperty

class TextItemInput() {
  @BeanProperty
  var tableName: String = _
  @BeanProperty
  var fieldTextId: String = _
  @BeanProperty
  var fieldOthers: String = _
  @BeanProperty
  var sql2Filter: String = _
}
object TextItemInput {
  val cliTableName: String = "tablename"
  val cliFieldTextId: String = "field_textid"
  val cliFieldOthers: String = "field_others"
  val cliSQL2Filter: String = "sql2filter"
}