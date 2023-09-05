/*
 * Copyright (c) 2021 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.workflow.service

//import io.opentelemetry.sdk.metrics.internal.export.MetricProducer
import com.fasterxml.jackson.annotation.JsonProperty
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.opendc.compute.service.scheduler.ComputeScheduler
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.service.scheduler.weights.VCpuWeigher
import org.opendc.experiments.compute.registerComputeMonitor
import org.opendc.experiments.compute.setupComputeService
import org.opendc.experiments.compute.setupHosts
//import org.opendc.experiments.compute.telemetry.table.HostTableReader
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
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import org.opendc.workflow.service.scheduler.job.NullJobAdmissionPolicy
import org.opendc.workflow.service.scheduler.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflow.service.scheduler.task.HEFTPolicy
import org.opendc.workflow.service.scheduler.task.NullTaskEligibilityPolicy
import org.opendc.workflow.service.scheduler.task.SubmissionTimeTaskOrderPolicy
import java.io.File
import java.nio.file.Paths
import java.time.Duration
import java.util.UUID

/**
 * Integration test suite for the [WorkflowService].
 */
@DisplayName("WorkflowService")
internal class WorkflowServiceTest {
    /**
     * A large integration test where we check whether all tasks in some trace are executed correctly.
     */
    @Test
    fun testTrace() = runSimulation {
        val computeService = "compute.opendc.org"
        val workflowService = "workflow.opendc.org"

        Provisioner(dispatcher, seed = 0L).use { provisioner ->
            val scheduler: (ProvisioningContext) -> ComputeScheduler = {
                FilterScheduler(
                    filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
                    weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
                )
            }
            val monitor = ResultingComputeMonitor()
            provisioner.runSteps(
                // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
                setupComputeService(computeService, scheduler, schedulingQuantum = Duration.ofSeconds(1)),
                registerComputeMonitor(serviceDomain = "compute.opendc.org", monitor, exportInterval = Duration.ofSeconds(1000)),
                setupHosts(computeService, getNodesSpecs()),

                // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
                setupWorkflowService(
                    workflowService,
                    computeService,
                    WorkflowSchedulerSpec(
                        schedulingQuantum = Duration.ofMillis(100),
                        jobAdmissionPolicy = NullJobAdmissionPolicy,
                        jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
                        taskEligibilityPolicy = NullTaskEligibilityPolicy,
                        taskOrderPolicy = SubmissionTimeTaskOrderPolicy()
                    )
                )
            )

            val service = provisioner.registry.resolve(workflowService, WorkflowService::class.java)!!

            val trace = Trace.open(
                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/trace.gwf")).toURI()),
                format = "gwf"
            )
            //service.replay(timeSource, trace.toJobs())

            coroutineScope {
                launch { service.replay(timeSource, trace.toJobs()) }
//                delay(10_000)
                val impl = (service as WorkflowServiceImpl)
                println(impl.jobQueue)
            }

            println(
                "Compute Metrics->" +
                    "Energy-Usage=${monitor.energyUsage} ")

            val metrics = service.getSchedulerStats()

            assertAll(
                { assertEquals(758, metrics.workflowsSubmitted, "No jobs submitted") },
                { assertEquals(0, metrics.workflowsRunning, "Not all submitted jobs started") },
                {
                    assertEquals(
                        metrics.workflowsSubmitted,
                        metrics.workflowsFinished,
                        "Not all started jobs finished"
                    )
                },
                { assertEquals(0, metrics.tasksRunning, "Not all started tasks finished") },
                { assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") },
//                { assertEquals(45977707L, timeSource.millis()) { "Total duration incorrect" } }
            )
        }
    }

    /**
     * Construct a [HostSpec] for a simulated host.
     */
    private fun createHostSpec(nodeUid: Int, cpuVendor: String, cpuModel: String, cpuArch: String, cpuCoreCount: Int,
                               cpuMaxFrequency: Double, memoryVendor: String, memoryModel: String, memorySpeed: Double, memorySize: Long): HostSpec {
        // Originally, machine model based on: https://www.spec.org/power_ssj2008/results/res2020q1/power_ssj2008-20191125-01012.html
        // Current machine model based on: https://www.cs.vu.nl/das/clusters.shtml, https://www.amd.com/en/products/cpu/amd-epyc-7402p
        val node = ProcessingNode(cpuVendor, cpuModel, cpuArch, cpuCoreCount)
        val cpus = List(node.coreCount) { ProcessingUnit(node, it, cpuMaxFrequency) }
        val memory = List(8) { MemoryUnit(memoryVendor, memoryModel, memorySpeed, memorySize/8) }
        // add storage and network in the machineModel below, maybe
        val machineModel = MachineModel(cpus, memory)
        val powerModel: CpuPowerModel = CpuPowerModels.linear(350.0, 200.0)
        return HostSpec(
            UUID(0, nodeUid.toLong()),
            "host-$nodeUid",
            emptyMap(),
            machineModel,
            SimPsuFactories.simple(powerModel),
            FlowMultiplexerFactory.forwardingMultiplexer()
        )
    }

    @Test
    internal fun printOutput() {
        println("................Printing the test-configurations..............")
        getNodesSpecs()
    }

    private fun getNodesSpecs() : List<HostSpec> {
        val mapper = jacksonObjectMapper()
        // Loading standard DAS-6 Host Configuration: https://www.cs.vu.nl/das/clusters.shtml
        val jsonString: String = File("/windows/os/Thesis-Project/opendc/config/node-config.json").readText(Charsets.UTF_8)
        val nodeConfig: NodeConfig = mapper.readValue<NodeConfig>(jsonString)
        val specs : List<HostSpec> = List(nodeConfig.totalNodesCount) { createHostSpec(it,nodeConfig.cpuConfig.cpuVendor, nodeConfig.cpuConfig.cpuModel,
            nodeConfig.cpuConfig.cpuArch, nodeConfig.cpuConfig.cpuCores, nodeConfig.cpuConfig.cpuMaxFrequency, nodeConfig.memoryConfig.memoryVendor,
            nodeConfig.memoryConfig.memoryModel, nodeConfig.memoryConfig.memorySpeed, nodeConfig.memoryConfig.memorySize) }
        println(nodeConfig.totalNodesCount)
        return specs
    }

    @Test
    internal fun conductExperiment() = runSimulation {
        val computeService = "compute.opendc.org"
        val workflowService = "workflow.opendc.org"

        Provisioner(dispatcher, seed = 0L).use { provisioner ->
            val scheduler: (ProvisioningContext) -> ComputeScheduler = {
                FilterScheduler(
                    filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
                    weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
                )
            }
            // read global config about environment setup
            val specs : List<HostSpec> = getNodesSpecs()
            val monitor = ResultingComputeMonitor()
            provisioner.runSteps(
                // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
                setupComputeService(computeService, scheduler, schedulingQuantum = Duration.ofSeconds(1)),
                registerComputeMonitor(serviceDomain = "compute.opendc.org", monitor, exportInterval = Duration.ofSeconds(1)),
                setupHosts(computeService, specs),

                // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
                setupWorkflowService(
                    workflowService,
                    computeService,
                    WorkflowSchedulerSpec(
                        schedulingQuantum = Duration.ofMillis(100),
                        jobAdmissionPolicy = NullJobAdmissionPolicy,
                        jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
                        taskEligibilityPolicy = NullTaskEligibilityPolicy,
                        taskOrderPolicy = HEFTPolicy(HashSet(specs))
                    )
                )
            )
        }
    }
}
