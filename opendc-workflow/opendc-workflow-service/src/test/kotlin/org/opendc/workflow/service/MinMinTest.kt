package org.opendc.workflow.service

import org.opendc.simulator.kotlin.runSimulation
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.opendc.experiments.compute.topology.HostSpec
import org.opendc.simulator.compute.SimPsuFactories
import org.opendc.simulator.compute.kernel.SimHypervisor
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.power.CpuPowerModel
import org.opendc.simulator.compute.power.CpuPowerModels
import org.opendc.workflow.api.Job
import org.opendc.workflow.api.Task
import org.opendc.workflow.api.WORKFLOW_TASK_CORES
import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.scheduler.task.MinMinPolicy
import java.util.*
import kotlin.collections.HashSet
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext

class MinMinTest {
    // ProcessingNode = CPU, ProcessingUnit = Core = vCPU

    @Test
    fun whenThereIsSingleHostWithSingleCore_TasksAreOrderedByExecutionTime() = runSimulation {
        val tasks = hashSetOf(
            Task(UUID(0L, 1L), "Task0", HashSet(),
                mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1)),
            Task(UUID(0L, 2L), "Task1", HashSet(),
                mutableMapOf("cpu-cycles" to 3000L, WORKFLOW_TASK_CORES to 1)),
            Task(UUID(0L, 3L), "Task2", HashSet(),
                mutableMapOf("cpu-cycles" to 2000L, WORKFLOW_TASK_CORES to 1)))

        val hostSpecs = mutableSetOf(createDefaultHostSpec(1))
        val minmin = MinMinPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = minmin.orderTasks(input)

        Assertions.assertEquals("Task0", orderedTasks.poll().task.name)
        Assertions.assertEquals("Task2", orderedTasks.poll().task.name)
        Assertions.assertEquals("Task1", orderedTasks.poll().task.name)
    }

    fun getAssignedHost(taskState: TaskState): String {
        return (taskState.task.metadata["assigned-host"] as Pair<UUID, String>).second
    }

    @Test
    fun whenThereAreTwoHostsWithSingleCoreForTwoTasks_EveryTaskIsAssignedToOneOfTheHosts() = runSimulation {
        val tasks = hashSetOf(
            Task(UUID(0L, 1L), "Task0", HashSet(),
                mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1)),
            Task(UUID(0L, 2L), "Task1", HashSet(),
                mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1)))

        val hostSpecs = mutableSetOf<HostSpec>(createDefaultHostSpec(0), createDefaultHostSpec(1))
        val minmin = MinMinPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = minmin.orderTasks(input)

        Assertions.assertEquals("host-0", getAssignedHost(orderedTasks.poll()))
        Assertions.assertEquals("host-1", getAssignedHost(orderedTasks.poll()))
    }

    @Test
    fun whenItIsFasterToExecuteTwoTasksOnSameHosts_BothTasksAreAssignedToThisHost() = runSimulation {
        val tasks = hashSetOf(
            Task(UUID(0L, 1L), "Task0", HashSet(),
                mutableMapOf("cpu-cycles" to 2000L, WORKFLOW_TASK_CORES to 1)),
            Task(UUID(0L, 2L), "Task1", HashSet(),
                mutableMapOf("cpu-cycles" to 500L, WORKFLOW_TASK_CORES to 1)))

        val cpu0 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores0 = listOf(ProcessingUnit(cpu0, 1, 3000.0))
        val cpu1 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores1 = listOf(ProcessingUnit(cpu1, 1, 500.0))
        val hostSpecs = mutableSetOf(
            createHostSpec(0, MachineModel(cores0, emptyList())),
            createHostSpec(1, MachineModel(cores1, emptyList())))

        val minmin = MinMinPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = minmin.orderTasks(input)

        val firstTask = orderedTasks.poll()
        val secondTask = orderedTasks.poll()

        // Task1 is selected first, since it has the shortest execution time
        Assertions.assertEquals("Task1", firstTask.task.name)
        Assertions.assertEquals("host-0", getAssignedHost(firstTask))
        // Task0 is executed on host-0 as well, since this host can execute it much faster than host-1
        Assertions.assertEquals("Task0", secondTask.task.name)
        Assertions.assertEquals("host-0", getAssignedHost(secondTask))
    }

    @Test
    fun whenThreeTasksWithDifferentRuntimesAreSchedules_StartTimesAreConsidered() = runSimulation {
        val tasks = hashSetOf(
            Task(UUID(0L, 1L), "Task0", HashSet(),
                mutableMapOf("cpu-cycles" to 750L, WORKFLOW_TASK_CORES to 1)),
            Task(UUID(0L, 2L), "Task1", HashSet(),
                mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1)),
            Task(UUID(0L, 3L), "Task2", HashSet(),
                mutableMapOf("cpu-cycles" to 1500L, WORKFLOW_TASK_CORES to 1)))

        val cpu0 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores0 = listOf(ProcessingUnit(cpu0, 1, 1000.0))
        val cpu1 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores1 = listOf(ProcessingUnit(cpu1, 1, 2000.0))
        val hostSpecs = mutableSetOf(
            createHostSpec(0, MachineModel(cores0, emptyList())),
            createHostSpec(1, MachineModel(cores1, emptyList())))

        val minmin = MinMinPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = minmin.orderTasks(input)

        val firstTask = orderedTasks.poll()
        val secondTask = orderedTasks.poll()
        val thirdTask = orderedTasks.poll()

        // Task0 is selected first, since it has the shortest execution time
        Assertions.assertEquals("Task0", firstTask.task.name)
        Assertions.assertEquals("host-1", getAssignedHost(firstTask))
        // Task1 is assigned to host-1, since 750/2000Hz (startTime) + 1000/2000Hz = 0.875s < 0 + 1000/1000Hz
        Assertions.assertEquals("Task1", secondTask.task.name)
        Assertions.assertEquals("host-1", getAssignedHost(secondTask))
        // Task2 is assigned to host-0, since 0 + 1500/1000Hz = 1.5s < 1750/2000Hz + 1500/2000Hz = 1.625s
        Assertions.assertEquals("Task2", thirdTask.task.name)
        Assertions.assertEquals("host-0", getAssignedHost(thirdTask))
    }

    @Test
    fun whenHostHasTwoCores_BothCoresAreUtilized() = runSimulation {
        val tasks = hashSetOf(
            Task(UUID(0L, 1L), "Task0", HashSet(),
                mutableMapOf("cpu-cycles" to 3000L, WORKFLOW_TASK_CORES to 1)),
            Task(UUID(0L, 2L), "Task1", HashSet(),
                mutableMapOf("cpu-cycles" to 3000L, WORKFLOW_TASK_CORES to 1)))

        val cpu0 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores0 = listOf(ProcessingUnit(cpu0, 1, 1000.0))
        val cpu1 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 2)
        val cores1 = listOf(ProcessingUnit(cpu1, 1, 2000.0), ProcessingUnit(cpu1, 2, 2000.0))
        val hostSpecs = mutableSetOf(
            createHostSpec(0, MachineModel(cores0, emptyList())),
            createHostSpec(1, MachineModel(cores1, emptyList())))

        val minmin = MinMinPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = minmin.orderTasks(input)

        val firstTask = orderedTasks.poll()
        val secondTask = orderedTasks.poll()

        Assertions.assertEquals("host-1", getAssignedHost(firstTask))
        Assertions.assertEquals("host-1", getAssignedHost(secondTask))
    }

    @Test
    fun whenTasksRequireMultipleCores_OnlyHostsWithEnoughCoresAreConsidered() = runSimulation {
        val tasks = hashSetOf(
            Task(UUID(0L, 1L), "Task0", HashSet(),
                mutableMapOf("cpu-cycles" to 3000L, WORKFLOW_TASK_CORES to 2)),
            Task(UUID(0L, 2L), "Task1", HashSet(),
                mutableMapOf("cpu-cycles" to 3000L, WORKFLOW_TASK_CORES to 2)))

        val cpu0 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores0 = listOf(ProcessingUnit(cpu0, 1, 3000.0))
        val cpu1 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 2)
        val cores1 = listOf(ProcessingUnit(cpu1, 1, 1000.0), ProcessingUnit(cpu1, 2, 1000.0))
        val hostSpecs = mutableSetOf(
            createHostSpec(0, MachineModel(cores0, emptyList())),
            createHostSpec(1, MachineModel(cores1, emptyList())))

        val minmin = MinMinPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = minmin.orderTasks(input)

        val firstTask = orderedTasks.poll()
        val secondTask = orderedTasks.poll()

        Assertions.assertEquals("host-1", getAssignedHost(firstTask))
        Assertions.assertEquals("host-1", getAssignedHost(secondTask))
    }

    /**
     * Returns a HostSpec, which contains one CPU with one core (clock frequency = 3400)
     * and no memory units.
     */
    private fun createDefaultHostSpec(uid: Int): HostSpec {
        val cpu = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores = listOf(ProcessingUnit(cpu, 1, 3400.0))

        val machineModel = MachineModel(cores, emptyList<MemoryUnit>())
        return createHostSpec(uid, machineModel)
    }

    private fun createHostSpec(uid: Int, machineModel: MachineModel): HostSpec {
        val powerModel: CpuPowerModel = CpuPowerModels.linear(350.0, 200.0) //testing
        return HostSpec(
            UUID(0, uid.toLong()),
            "host-$uid",
            emptyMap(),
            machineModel,
            SimPsuFactories.simple(powerModel) //SimplePowerDriver(ConstantPowerModel(0.0)),
            // optional SimSpaceSharedHypervisorProvider()
        )
    }

    private fun createInputForPolicy(tasks: HashSet<Task>, context: CoroutineContext): HashSet<TaskState> {
        val cont = Continuation<Unit>(context) { ; }
        val job = JobState(Job(UUID.randomUUID(), "onlyJob", tasks), 0, cont)
        return tasks.map({ TaskState(job, it) }).toHashSet()
    }


    //@Test
//    fun testTrace() = runSimulation {
//        val computeScheduler = AssignmentExecutionScheduler()
//        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))
//
//        val HOST_COUNT = 4
//        val hostSpecs = HashSet<HostSpec>()
//        repeat(HOST_COUNT) {
//            val cpu = ProcessingNode("AMD", "am64", "EPYC 7742", 32)
//            val cores = List(cpu.coreCount) { ProcessingUnit(cpu, it, 3400.0) }
//            val memory = List(8) { MemoryUnit("Samsung", "Unknown", 2933.0, 16_000) }
//            val machineModel = MachineModel(cores, memory)
//
//            val hostSpec = HostSpec(
//                UUID(0, it.toLong()), "host-$it", emptyMap(), machineModel,
//                SimplePowerDriver(ConstantPowerModel(0.0)), SimSpaceSharedHypervisorProvider())
//            computeHelper.registerHost(hostSpec)
//            hostSpecs.add(hostSpec)
//        }
//
//        // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
//        val workflowScheduler = WorkflowSchedulerSpec(
//            schedulingQuantum = Duration.ofMillis(100),
//            jobAdmissionPolicy = NullJobAdmissionPolicy,
//            jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
//            taskEligibilityPolicy = NullTaskEligibilityPolicy,
//            taskOrderPolicy = MinMinPolicy(hostSpecs),
//        )
//        val workflowHelper = WorkflowServiceHelper(coroutineContext, clock, computeHelper.service.newClient(), workflowScheduler)
//
//        try {
//            val trace = Trace.open(
//                Paths.get(checkNotNull(WorkflowServiceTest_Florian::class.java.getResource("/trace.gwf")).toURI()),
//                format = "gwf"
//            )
//
//            val jobs = trace.toJobs().take(100)
//            workflowHelper.replay(jobs)
//        } finally {
//            workflowHelper.close()
//            computeHelper.close()
//        }
//
//
//        val metrics = collectMetrics(workflowHelper.metricProducer)
//
//        assertAll(
//            { Assertions.assertEquals(100, metrics.jobsSubmitted, "No jobs submitted") },
//            { Assertions.assertEquals(0, metrics.jobsActive, "Not all submitted jobs started") },
//            { Assertions.assertEquals(metrics.jobsSubmitted, metrics.jobsFinished, "Not all started jobs finished") },
//            { Assertions.assertEquals(0, metrics.tasksActive, "Not all started tasks finished") },
//            { Assertions.assertEquals(3590, metrics.tasksSubmitted, "Not all tasks were submitted") },
//            { Assertions.assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") }
//        )
//    }
//
//    private fun collectMetrics(metricProducer: MetricProducer): WorkflowServiceTest_Florian.WorkflowMetrics {
//        val metrics = metricProducer.collectAllMetrics().associateBy { it.name }
//        val res = WorkflowServiceTest_Florian.WorkflowMetrics()
//        res.jobsSubmitted = metrics["jobs.submitted"]?.longSumData?.points?.last()?.value ?: 0
//        res.jobsActive = metrics["jobs.active"]?.longSumData?.points?.last()?.value ?: 0
//        res.jobsFinished = metrics["jobs.finished"]?.longSumData?.points?.last()?.value ?: 0
//        res.tasksSubmitted = metrics["tasks.submitted"]?.longSumData?.points?.last()?.value ?: 0
//        res.tasksActive = metrics["tasks.active"]?.longSumData?.points?.last()?.value ?: 0
//        res.tasksFinished = metrics["tasks.finished"]?.longSumData?.points?.last()?.value ?: 0
//        return res
//    }
}


