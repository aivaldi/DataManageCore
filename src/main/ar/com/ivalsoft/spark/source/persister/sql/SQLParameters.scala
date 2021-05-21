package ar.com.ivalsoft.spark.source.persister.sql

import java.util.Properties
import org.apache.spark.sql.SaveMode
import ar.com.ivalsoft.spark.source.persister.DefaultParameters
import org.apache.log4j.Logger

/**
 * @author aivaldi
 * 
 * es un wrapper para la conexion a la base de datos 
 * 
 * MS no iria peor no quiero refactorizar
 *
 */
class SQLParameters extends DefaultParameters {
   
  
  val log = Logger.getLogger(getClass.getName)
  
  private var _dataBaseType:String=""
  private var _server:String=""
  private var _dataBaseName:String=""
  private var _tableName:String=""
  private var _user:String=""
  private var _password:String=""
  private var _saveMode:SaveMode=SaveMode.Append
  private var _deleteValues:Boolean=false
  private var _port:String=""
  private var _integratedSecurity:Boolean=false
  
  def server:String = _server; 
  def server_= (value:String):Unit = _server = value
  
  def dataBaseName:String = _dataBaseName;
  def dataBaseName_= (value:String):Unit = _dataBaseName = value
  
  def tableName:String = _tableName;
  def tableName_= (value:String):Unit = _tableName = value
  
  def user:String = _user;
  def user_= (value:String):Unit = _user = value
  
  def password:String = _password;
  def password_= (value:String):Unit = _password = value
  
  def saveMode:SaveMode = _saveMode; 
  def saveMode_= (value:SaveMode):Unit = _saveMode = value
  
  def deleteValues:Boolean = _deleteValues; 
  def deleteValues_= (value:Boolean):Unit = _deleteValues = value
  
  def dataBaseType:String = _dataBaseType; 
  def dataBaseType_= (value:String):Unit = _dataBaseType = value
 
  def port:String = _port; 
  def port_= (value:String):Unit = _port = value
 
  def integratedSecurity:Boolean= _integratedSecurity; 
  def integratedSecurity_= (value:Boolean):Unit = _integratedSecurity = value
  
  
  def getProperties:Properties = {
    var properties = new java.util.Properties
   
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    
    properties
    
  }
        
}