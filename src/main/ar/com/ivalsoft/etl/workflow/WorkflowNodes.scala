package ar.com.ivalsoft.etl.workflow

import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.Executor

/**
 * @author aivaldi
 */
class WorkflowNodes {
  /***
   * Defino el tipo de nodo que representa
   */
  var nType:String=""
  /**
   * Parametros del nodo
   */
  var params:Params=null
  /**
   * Id de identificacion
   */
  var id:String=""
  
  var level:Int=0
  
}