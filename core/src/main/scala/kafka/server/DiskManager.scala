package kafka.server

import kafka.utils.Logging
import org.apache.kafka.server.util.KafkaScheduler

import java.io.File
import java.nio.file.Files
import java.util.concurrent.ScheduledFuture

class DiskManager(
                   checkIntervalMs: Long,
                   maxThroughputBytes: Long,
                   logDir: String,
                   scheduler: KafkaScheduler,
                   lifecycleManager: BrokerLifecycleManager
                 ) extends Logging {

  private var diskCheckTask: ScheduledFuture[_] = _
  private var insufficientSpace = false

  def startup(): Unit = {
    if (checkIntervalMs > 0) {
      info(s"[DISK-MANAGER] Starting with checkInterval=$checkIntervalMs ms and maxThroughput=${maxThroughputBytes/1024/1024} MB/s")
      info(s"[DISK-MANAGER] Will fence broker if free space drops below ${(maxThroughputBytes * (checkIntervalMs/1000.0))/1024/1024} MB")
      diskCheckTask = scheduler.schedule(
        "disk-check",
        () => checkDiskSpace,
        0,
        checkIntervalMs
      )
    } else {
      info("[DISK-MANAGER] Disabled")
    }
  }

  private def checkDiskSpace(): Unit = {
    try {
      val criticalThresholdBytes = maxThroughputBytes * ((checkIntervalMs * 2) / 1000.0)
      info(s"[DISK-MANAGER] Checking disk space. Critical threshold: ${criticalThresholdBytes/1024/1024} MB")

      val dirFile = new File(logDir)
      val store = Files.getFileStore(dirFile.toPath)
      val freeSpaceBytes = store.getUsableSpace

      info(s"[DISK-MANAGER] Log directory ${dirFile.getAbsolutePath}: " +
        s"free space = ${freeSpaceBytes/1024/1024} MB, " +
        s"critical threshold = ${criticalThresholdBytes/1024/1024} MB")

      insufficientSpace = freeSpaceBytes < criticalThresholdBytes
      if (insufficientSpace) {
        warn(s"[DISK-MANAGER] Log directory has insufficient space! " +
          s"Free: ${freeSpaceBytes/1024/1024} MB, Required: ${criticalThresholdBytes/1024/1024} MB")
        lifecycleManager.setWantFence()
      }
      else {
        lifecycleManager.setReadyToUnfence()
      }

    } catch {
      case e: Exception =>
        error("[DISK-MANAGER] Error during disk space check", e)
    }
  }


  def shutdown(): Unit = {
    if (checkIntervalMs > 0) {
      info("[DISK-MANAGER] Shutting down")
      if (diskCheckTask != null) {
        diskCheckTask.cancel(false)
      }
    }
  }
}