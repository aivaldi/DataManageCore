package ar.com.ivalsoft.etl.executor.function.helper

import ar.com.ivalsoft.etl.executor.EtlSqlContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
object ETLFunctions {
        
    def registerFunctions(sc:SparkSession) = {
      
      var sqlContext = sc.sqlContext
  
        sqlContext.udf.register("test", (element:String,value:Seq[String],formula:Seq[Any] ) =>{ 
         try{
           
           var ret:Any=0l
           (value zip formula).foreach { 
             case (va, form) => if (va.equals(element))
                                 ret = form;
               } 
           ret.toString()
           
         }catch{
           case e:Exception => {
             null 
           }
         }
       }
     )
     
    }    
}