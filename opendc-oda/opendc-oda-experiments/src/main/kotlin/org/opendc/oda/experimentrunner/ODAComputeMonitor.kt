package org.opendc.oda.experimentrunner

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.opendc.experiments.compute.telemetry.ComputeMonitor
import org.opendc.experiments.compute.telemetry.table.HostTableReader
import java.time.Instant
import java.time.LocalDateTime

class ODAComputeMonitor (traceFile: String, policyName: String, percentage: Int, isAnomalous: Boolean?) : ComputeMonitor {
    var policy: String
    var trace: String
    var anomalyState: String
    private var producer: org.apache.kafka.clients.producer.Producer<String, ByteArray>?
    init {
        // get kafka-Producer instance from the static class
        producer = ODAExperimentKafkaProducer.getKafkaProducerInstance()
        if(traceFile.startsWith("/shell"))
            trace = "Shell"
        else if(traceFile.startsWith("/Pegasus"))
            trace = "Pegasus"
        else if(traceFile.startsWith("/askalon"))
            trace = "Askalon"
        else
            trace = ""
        if(isAnomalous == null)
            anomalyState = ""
        else if(isAnomalous)
            anomalyState = "-Anomaly"
        else
            anomalyState = "-NonAnomaly"
        policy = policyName + "-" + trace + anomalyState + "-" + percentage
        println("Selected scheduling configuration for experiment = ${policy}")
//        if(producer!=null)
//            println("Kafka producer reference also successfully obtained!")
    }
    var idleTime = 0L
    var activeTime = 0L
    var stealTime = 0L
    var lostTime = 0L
    var energyUsage = 0.0
    var uptime = 0L
    var host_id = 0
    var timestamp : Instant = Instant.now()
    // lateinit var timestampInstant : Instant
    var cpuUtilisation : Double = 0.0
    var powerusage : Double = 0.0
    var guestsRunning : Int = 0
    var now = LocalDateTime.now()
    var policy_id: Int = 0

    override fun record(reader: HostTableReader) {
        idleTime += reader.cpuIdleTime
        activeTime += reader.cpuActiveTime
        stealTime += reader.cpuStealTime
        lostTime += reader.cpuLostTime
        energyUsage += reader.powerTotal
        uptime += reader.uptime
        host_id = reader.host.name.substringAfterLast("-").toInt() // left-trim "host-" at the beginning
        timestamp = reader.timestamp
        cpuUtilisation = reader.cpuUtilization
        powerusage = reader.powerUsage
        guestsRunning = reader.guestsRunning

        when (policy) {
            "FIFO-Shell-100" -> policy_id = 1
            "HEFT-Shell-100" -> policy_id = 2
            "Random-Shell-100" -> policy_id = 3
            "FIFO-Pegasus-100" -> policy_id = 4
            "HEFT-Pegasus-100" -> policy_id = 5
            "Random-Pegasus-100" -> policy_id = 6
//            "MinMin" -> policy_id = 4
            "FIFO-Shell-60" -> policy_id = 61
            "HEFT-Shell-60" -> policy_id = 62
            "Random-Shell-60" -> policy_id = 63
            "FIFO-Pegasus-60" -> policy_id = 64
            "HEFT-Pegasus-60" -> policy_id = 65
            "Random-Pegasus-60" -> policy_id = 66

            "FIFO-Shell-150" -> policy_id = 151
            "HEFT-Shell-150" -> policy_id = 152
            "Random-Shell-150" -> policy_id = 153
            "FIFO-Pegasus-150" -> policy_id = 154
            "HEFT-Pegasus-150" -> policy_id = 155
            "Random-Pegasus-150" -> policy_id = 156

            "FIFO-Shell-NonAnomaly-100" -> policy_id = 1000
            "FIFO-Shell-Anomaly-100" -> policy_id = 1001
            "FIFO-Shell-NonAnomaly-150" -> policy_id = 1050
            "FIFO-Shell-Anomaly-150" -> policy_id = 1051
            else -> { // Note the block
                policy_id = 0
            }
        }
        try{
            val message =  MonitoringMetrics(idleTime, activeTime, lostTime, energyUsage, uptime, host_id, timestamp.epochSecond, cpuUtilisation, powerusage, guestsRunning, policy_id)
            val objectMapper = ObjectMapper()
            val jsonBytes = objectMapper.writeValueAsBytes(message)
            producer!!.send(ProducerRecord<String, ByteArray>(policy, jsonBytes))
//            print("Current-timestamp = ${timestamp.epochSecond}")
//            print("; Current-host-id = ${host_id}")
//            print("; Current-power-usage = ${powerusage}")
//            print("; Current-guests-running = ${guestsRunning}")
//            print("; Total-power-usage = ${energyUsage}")
//            print("; Kafka delivered")
//            println()
        }
        catch(e: Exception){
            println(e.stackTraceToString())
        }
    }

}

data class MonitoringMetrics (
    var cpuIdleTime: Long,
    var cpuActiveTime : Long,
    var cpuLostTime: Long,
    var powerTotal: Double,
    var uptime: Long,
    var serverId: Int,
    var timestamp: Long,
    var cpuUtilisation: Double,
    var powerUsage: Double,
    var guestsRunning: Int,
    var policyId: Int
)

