package ar.com.ivalsoft.etl.executor.join

import org.apache.spark.SparkContext
import scala.concurrent.ExecutionContext
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.join.params.JoinParams
import org.apache.spark.sql.Column
import org.apache.log4j.Logger
import ar.com.ivalsoft.etl.executor.params.ColumnClausePair
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class Join (implicit sc: SparkSession, ec:ExecutionContext) extends Executor with ExecutorIn {
  
    val log = Logger.getLogger(getClass.getName)

    def execute:Future[Option[DataFrame]] = {
        if (currentFuture!=null)
           return currentFuture;  
        else{   
         log.info("Creating Join")
         
         if (dfListIn.length!=2)
           throw new IllegalArgumentException("DataFrame to execute must be equal to 2")
         
         
         if (_param==null)
           throw new IllegalArgumentException("No columns selected")
         
         if (_param.asInstanceOf[JoinParams].joinType==null)
           throw new IllegalArgumentException("No columns selected")
         
         if (_param.asInstanceOf[JoinParams].joinColumns.size==0)
           throw new IllegalArgumentException("No join criteria")
         
         
         if ( !Seq("I","L","R").exists { x => x== _param.asInstanceOf[JoinParams].joinType} )
           throw new IllegalArgumentException("No correct join type detected, found:"+_param.asInstanceOf[JoinParams].joinType+" needed: I,L,R")
         
         var joinType:String="inner"
         
         _param.asInstanceOf[JoinParams].joinType match {
           case "L" => joinType="left_outer"
           case "R" => joinType="right_outer"
           case "I" => joinType="inner"
         } 
         
         val df1 = dfListIn(0)
         val df2 = dfListIn(1)
         
         val w = Future.sequence(Seq(df1, df2) )
      
         
         w.flatMap {   
            res => 
              log.info("Executing Join")
              val res1 = res(0)
              val res2 = res(1)
              
              val squJoinSelectOrigin = 
                 _param.asInstanceOf[JoinParams].joinColumns                  
                  .map { 
                    jc=> jc.columnOrigin
                  } toSeq
              
              
              val squJoinSelectJoin= 
                 _param.asInstanceOf[JoinParams].joinColumns                  
                  .map { 
                    jc=> jc.columnJoin
                  } toSeq
              
              
              import org.apache.spark.sql.functions.trim
              //obtengo los campos del join 
               
              
              //Selecciono las columnas de cada uno que quiero
              val selectOrigin = (_param.asInstanceOf[JoinParams].columnSelected
                    .filter { x => x.table.get=="columnOrigin" }
                      .map { colSel =>  if (colSel.columnName==colSel.columnAlias) colSel.columnName else s"${colSel.columnName} as ${colSel.columnAlias}"  }  toSeq )               
              
              val selectJoin =  (_param.asInstanceOf[JoinParams].columnSelected
                    .filter { x => x.table.get=="columnJoin" }
                      .map { colSel =>  if (colSel.columnName==colSel.columnAlias) colSel.columnName else s"${colSel.columnName} as ${colSel.columnAlias}"  }  toSeq )               
         
              val endSelection = (_param.asInstanceOf[JoinParams].columnSelected
                      .map { colSel =>  colSel.columnAlias  }  toSeq )               
         
                      
              log.info(s"Query for Join Origin ${squJoinSelectOrigin}")
              log.info(s"Query for Join Join${squJoinSelectJoin}")
              
              log.info(s"Query for result Origin ${selectOrigin}")
              log.info(s"Query for result Join ${selectJoin}")
              //join
              val left = res1.get.selectExpr( (squJoinSelectOrigin ++ selectOrigin).distinct:_* )
              val right = res2.get.selectExpr((squJoinSelectJoin++selectJoin).distinct:_*)
              var retDF = left.join(right, 
                    (_param.asInstanceOf[JoinParams].joinColumns.map{x=> createCondition(x,left,right)   } toSeq).
                      reduce( (c1,c2) => c1.and(c2)  ), 
                        joinType)
              
                        
              val leftCols = left.columns
              val rightCols = right.columns
              val commonCols = leftCols.toSet intersect rightCols.toSet

                      
              currentFuture = Future{
                              Some(retDF.select(leftCols.collect { case c if commonCols.contains(c) => left(c) } ++
                                  leftCols.collect { case c if !commonCols.contains(c) => left(c) } ++
                                  rightCols.collect { case c if !commonCols.contains(c) => right(c) } : _*)
                                    .selectExpr( (endSelection):_*))
                      }
          
             currentFuture;   
         
         }
    }
          
   }
    
    def createCondition(x:ColumnClausePair, left:DataFrame, right:DataFrame):Column ={
      import org.apache.spark.sql.functions.trim
      
      x.operation match{
      
        case "==" => trim(left.col( x.columnOrigin )).equalTo( trim(right.col(x.columnJoin)))
        case ">" => trim(left.col( x.columnOrigin )).gt(trim(right.col(x.columnJoin)))
        case "<" => trim(left.col( x.columnOrigin )).lt( trim(right.col(x.columnJoin)))
        case ">=" => trim(left.col( x.columnOrigin )).geq(trim(right.col(x.columnJoin)))
        case "<=" => trim(left.col( x.columnOrigin )).leq( trim(right.col(x.columnJoin)))
        case "<>" => trim(left.col( x.columnOrigin )).notEqual(trim(right.col(x.columnJoin)))
        case _ => trim(left.col( x.columnOrigin )).equalTo( trim(right.col(x.columnJoin)))
      }
      
      
    
      
    }
}