package org.opendc.oda.experimentrunner

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.service.scheduler.weights.VCpuWeigher
import org.opendc.experiments.compute.registerComputeMonitor
import org.opendc.experiments.compute.setupComputeService
import org.opendc.experiments.compute.setupHosts
import org.opendc.experiments.compute.topology.HostSpec
import org.opendc.experiments.provisioner.Provisioner
import org.opendc.experiments.provisioner.ProvisioningContext
import org.opendc.experiments.workflow.WorkflowSchedulerSpec
import org.opendc.experiments.workflow.replay
import org.opendc.experiments.workflow.setupWorkflowService
import org.opendc.experiments.workflow.toJobs
import org.opendc.simulator.compute.SimPsuFactories
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.power.CpuPowerModel
import org.opendc.simulator.compute.power.CpuPowerModels
import org.opendc.simulator.flow2.mux.FlowMultiplexerFactory
import org.opendc.simulator.kotlin.runSimulation
import org.opendc.trace.Trace
import org.opendc.workflow.service.WorkflowService
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import org.opendc.workflow.service.scheduler.job.NullJobAdmissionPolicy
import org.opendc.workflow.service.scheduler.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflow.service.scheduler.task.HEFTPolicy
import org.opendc.workflow.service.scheduler.task.NullTaskEligibilityPolicy
import org.opendc.workflow.service.scheduler.task.RandomTaskOrderPolicy
import org.opendc.workflow.service.scheduler.task.SubmissionTimeTaskOrderPolicy
import org.opendc.workflow.service.scheduler.task.TaskOrderPolicy
import java.io.File
import java.nio.file.Paths
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Callable

fun main() {
    SchedulingAlgorithmComparatorExperiment().triggerExperiment()
}

//@DisplayName("ODASchedulingAlgorithmEfficiencyExperimentRunnerClass")
public class SchedulingAlgorithmComparatorExperiment {

    fun triggerExperiment(){
        val traceFiles: List<String> = listOf<String>("/shell_parquet", "/Pegasus_P2_parquet") //
        val policyNames: List<String> = listOf<String>("FIFO","HEFT","Random")
        val percentages: List<Int> = listOf<Int>(100, 60, 150)
        for (traceFile in traceFiles){
            for(policyName in policyNames){
                for(percentage in percentages){
                    conductSchedulingRelatedExperiment(traceFile, policyName, percentage)
                    println("--------------------Experiment Done for ${traceFile} - ${policyName} - ${percentage}----------------------")
                }
            }
        }
    }

    // used https://stackoverflow.com/questions/45315666/converting-callablet-java-method-to-kotlin for Callable in Kotlin
//    private fun <T> createCallable(task: Callable<T>): Callable<T> {
//        return object : Callable<T> {
//            override fun call(): T  {
//                try {
//                    return task.call()
//                } catch (e: Exception) {
//                    //handle(e)
//                    throw e
//                }
//            }
//        }
//    }

    private fun createHostSpec(nodeUid: Int, cpuVendor: String, cpuModel: String, cpuArch: String, cpuCoreCount: Int,
                               cpuMaxFrequency: Double, memoryVendor: String, memoryModel: String, memorySpeed: Double, memorySize: Long): HostSpec {
        // Originally, machine model based on: https://www.spec.org/power_ssj2008/results/res2020q1/power_ssj2008-20191125-01012.html
        // Current machine model based on: https://www.cs.vu.nl/das/clusters.shtml, https://www.amd.com/en/products/cpu/amd-epyc-7402p
        val node = ProcessingNode(cpuVendor, cpuModel, cpuArch, cpuCoreCount)
        val cpus = List(node.coreCount) { ProcessingUnit(node, it, cpuMaxFrequency) }
        val memory = List(8) { MemoryUnit(memoryVendor, memoryModel, memorySpeed, memorySize/8) }
        // add storage and network in the machineModel below, maybe
        val machineModel = MachineModel(cpus, memory)
        val powerModel: CpuPowerModel = CpuPowerModels.cubic(350.0, 200.0)
        return HostSpec(
            UUID(0, nodeUid.toLong()),
            "host-$nodeUid",
            emptyMap(),
            machineModel,
            SimPsuFactories.simple(powerModel),
            FlowMultiplexerFactory.forwardingMultiplexer()
        )
    }

    private fun getNodesSpecs(percentage: Int = 100) : List<HostSpec> {
        val mapper = jacksonObjectMapper()
        // Loading standard DAS-6 Host Configuration: https://www.cs.vu.nl/das/clusters.shtml
        val jsonString: String = File("/windows/os/Thesis-Project/opendc/config/node-config.json").readText(Charsets.UTF_8)
        val nodeConfig: OpenDCNodeConfig = mapper.readValue<OpenDCNodeConfig>(jsonString)
        val totalNodes: Int = (nodeConfig.totalNodesCount * (0.01 * percentage)).toInt()
        println("Total nodes for this experiment = ${totalNodes}")
        val specs : List<HostSpec> = List(totalNodes) { createHostSpec(it,nodeConfig.cpuConfig.cpuVendor, nodeConfig.cpuConfig.cpuModel,
            nodeConfig.cpuConfig.cpuArch, nodeConfig.cpuConfig.cpuCores, nodeConfig.cpuConfig.cpuMaxFrequency, nodeConfig.memoryConfig.memoryVendor,
            nodeConfig.memoryConfig.memoryModel, nodeConfig.memoryConfig.memorySpeed, nodeConfig.memoryConfig.memorySize) }
        return specs
    }

    private fun conductSchedulingRelatedExperiment(traceFile: String, policyName: String, percentage: Int) = runSimulation {
        val computeService = "compute.opendc.org"
        val workflowService = "workflow.opendc.org"
        // read global config about environment setup
        val specs : List<HostSpec> = getNodesSpecs(percentage)
        val metricExportingQuantum: Long = 30

        var policy: Any? = null
        when (policyName) {
            "FIFO" -> policy = SubmissionTimeTaskOrderPolicy()
            "HEFT" -> policy = HEFTPolicy(HashSet(specs))
            "Random" -> policy = RandomTaskOrderPolicy
            else -> { // Note the block
                policy = SubmissionTimeTaskOrderPolicy()
            }
        }
        val policyFinaliser: TaskOrderPolicy = policy as TaskOrderPolicy

        Provisioner(dispatcher, seed = 0L).use { provisioner ->
            val scheduler: (ProvisioningContext) -> ComputeScheduler = {
                FilterScheduler(
                    filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
                    weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
                )
            }
            val isAnomalousExperiment: Boolean? = null
            val isFaultInjected = if (isAnomalousExperiment != null) isAnomalousExperiment else false
            println("Faulty experimental run is configured = ${isFaultInjected}")
            val monitor = ODAComputeMonitor(traceFile, policyName, percentage, isAnomalousExperiment)
            provisioner.runSteps(
                // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
                setupComputeService(computeService, scheduler, schedulingQuantum = Duration.ofSeconds(1), isFaultInjected = isFaultInjected),
                registerComputeMonitor(serviceDomain = "compute.opendc.org", monitor, exportInterval = Duration.ofSeconds(metricExportingQuantum)),
                setupHosts(computeService, specs, isFaultInjected = isFaultInjected),

                // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
                setupWorkflowService(
                    workflowService,
                    computeService,
                    WorkflowSchedulerSpec(
                        schedulingQuantum = Duration.ofMillis(100),
                        jobAdmissionPolicy = NullJobAdmissionPolicy,
                        jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
                        taskEligibilityPolicy = NullTaskEligibilityPolicy,
                        taskOrderPolicy = policyFinaliser
                    )
                )
            )

            val service = provisioner.registry.resolve(workflowService, WorkflowService::class.java)!!
            service.isFaultInjected = true
            val trace = Trace.open(
                Paths.get(checkNotNull(SchedulingAlgorithmComparatorExperiment::class.java.getResource(traceFile)).toURI()),
                format = "wtf"
            )
            coroutineScope {
                launch { service.replay(timeSource, trace.toJobs()) }
                //val impl = (service as WorkflowServiceImpl)
            }
        }
    }
}
