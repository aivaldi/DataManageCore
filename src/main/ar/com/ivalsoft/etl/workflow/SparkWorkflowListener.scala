package ar.com.ivalsoft.etl.workflow

import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerApplicationEnd

/**
 * @author aivaldi
 */
class SparkWorkflowListener extends SparkListener(){
     
 
     
     /**
     * Called when the application starts
     */
    override def onApplicationStart(applicationStart: SparkListenerApplicationStart) { 
      println(s"AppStart ${applicationStart.appName}")
      
    }

    /**
     * Called when the application ends
     */
    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) { 
      println(s"AppEnd${applicationEnd.time}")
      
    }

    override  def onJobEnd(jobEnd: SparkListenerJobEnd) ={
       println(s"Job Ended ${jobEnd.jobId}")
       
     }
     override  def onJobStart(jobStart: SparkListenerJobStart) ={
       println(s"Job Started ${jobStart.jobId}, number of stages ${jobStart.stageIds.length}")
       jobStart.properties.keySet.toArray().foreach { println }
     }
     override def onStageCompleted(stageCompleted: SparkListenerStageCompleted){
       println(s"stage completed ${stageCompleted.stageInfo.name}" )
       println(s"stage parents ID ${stageCompleted.stageInfo.parentIds}" )
       
     }
     
     override def onTaskStart(arg: SparkListenerTaskStart) {
       println(s"taskStart ${arg.taskInfo.executorId}" )
      }
     override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted){
       println(s"stage submited ${stageSubmitted.stageInfo.name}" )
       println(s"stage parents ID ${stageSubmitted.stageInfo.parentIds}" )
       
     }
     
   
}