package ar.com.ivalsoft.etl.workflow

/**
 * @author aivaldi
 */
class WorkflowLinks {
  
  /***
   * Defino el tipo de nodo que representa
   */
  var nodeId:String=null
  
  var level:Int=0
  
  var in:Seq[String] = null
  
  var out:Seq[String] = null
  
}