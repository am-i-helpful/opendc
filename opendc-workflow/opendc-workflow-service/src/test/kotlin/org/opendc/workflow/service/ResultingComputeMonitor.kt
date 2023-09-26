package org.opendc.workflow.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.opendc.experiments.compute.telemetry.ComputeMonitor
import org.opendc.experiments.compute.telemetry.table.HostTableReader
import java.time.Instant
import java.time.LocalDateTime
import java.util.Properties


class ResultingComputeMonitor (policyName: String) : ComputeMonitor {
    val policy: String
    private val BOOTSTRAP_SERVERS = "localhost:9092"
    private var producer: org.apache.kafka.clients.producer.Producer<String, ByteArray>? = null
    init {
        policy = policyName
        println("Selected scheduling policy = ${policy}")
        val properties = Properties()
        properties["bootstrap.servers"] = BOOTSTRAP_SERVERS // Replace with your Kafka broker(s)
        properties["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        properties["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
        producer = KafkaProducer<String, ByteArray>(properties)
        println("Initialised Kafka producer")
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
        //timestampInstant = reader.timestamp
        timestamp = reader.timestamp
        cpuUtilisation = reader.cpuUtilization
        powerusage = reader.powerUsage
        guestsRunning = reader.guestsRunning
        print("Current-timestamp = ${timestamp.epochSecond}")
        print("; Current-host-id = ${host_id}")
        print("; Current-power-usage = ${powerusage}")
        print("; Current-guests-running = ${guestsRunning}")
        print("; CPU-active-time = ${activeTime}")
        when (policy) {
            "Standard" -> policy_id = 1
            "HEFT" -> policy_id = 2
            "Random" -> policy_id = 3
            "MinMin" -> policy_id = 4
            "Standard-Anomaly" -> policy_id = 5
            "Standard-NonAnomaly" -> policy_id = 6
            else -> { // Note the block
                policy_id = 0
            }
        }
        val message =  JsonMessage(idleTime, activeTime, lostTime, energyUsage, uptime, host_id, timestamp.epochSecond, cpuUtilisation, powerusage, guestsRunning, policy_id)
        val objectMapper = ObjectMapper()
        val jsonBytes = objectMapper.writeValueAsBytes(message)
        // producer!!.send(ProducerRecord<String, ByteArray>(policy, jsonBytes))
        print("; Kafka delivered")
        println()
    }

}

data class JsonMessage (
    var cpuIdleTime: Long,
    var cpuActiveTime : Long,
    var cpuLostTime: Long,
    var powerTotal: Double,
    var uptime: Long,
    var host_id: Int,
    var timestamp: Long,
    var cpuUtilisation: Double,
    var powerUsage: Double,
    var guestsRunning: Int,
    var policy_id: Int
)
