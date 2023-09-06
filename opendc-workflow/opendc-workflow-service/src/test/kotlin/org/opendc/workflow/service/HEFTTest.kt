package org.opendc.workflow.service

//import io.opentelemetry.sdk.metrics.export.MetricProducer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
//import org.opendc.compute.workload.topology.HostSpec
import org.opendc.experiments.compute.topology.HostSpec
import org.opendc.simulator.compute.SimPsuFactories
//import org.opendc.simulator.compute.kernel.SimHypervisor
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
//import org.opendc.simulator.compute.power.ConstantPowerModel
import org.opendc.simulator.compute.power.CpuPowerModel
import org.opendc.simulator.compute.power.CpuPowerModels
//import org.opendc.simulator.compute.power.SimplePowerDriver
import org.opendc.simulator.kotlin.runSimulation
import org.opendc.workflow.api.Job
import org.opendc.workflow.api.Task
import org.opendc.workflow.api.WORKFLOW_TASK_CORES
import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.scheduler.task.HEFTPolicy
import java.util.*
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext

@Suppress("LocalVariableName")
class HEFTTest {
    // ProcessingNode = CPU, ProcessingUnit = Core = vCPU

    @Test
    fun whenThereIsSingleHostWithSingleCore_TasksAreOfTypeBothDependentAndIndependent() = runSimulation {
        val task0 = Task(UUID(0L, 1L), "Task0", HashSet(), mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1))
        val task1 = Task(UUID(0L, 2L), "Task1", setOf(task0), mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1))
        val task2 = Task(UUID(0L, 3L), "Task2", HashSet(), mutableMapOf("cpu-cycles" to 1100L, WORKFLOW_TASK_CORES to 1))

        val tasks = setOf(
            task0,
            task1,
            task2)

        val hostSpecs = mutableSetOf(createDefaultHostSpec(1))
        val heft = HEFTPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = heft.orderTasks(input)

        Assertions.assertEquals("Task0", orderedTasks.poll().task.name)
        Assertions.assertEquals("Task2", orderedTasks.poll().task.name)
        Assertions.assertEquals("Task1", orderedTasks.poll().task.name)
    }

    fun getAssignedHost(taskState: TaskState): String {
        return (taskState.task.metadata["assigned-host"] as Pair<UUID, String>).second
    }

    @Test
    fun whenThereAreTwoHostsWithSingleCoreForTwoTasks_EveryTaskIsIndependentAndAssignedToOneOfTheHosts() = runSimulation {
        val task1 = Task(UUID(0L, 1L), "Task0", HashSet(), mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1))
        val task2 = Task(UUID(0L, 2L), "Task1", HashSet(), mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1))
        val tasks = setOf(
            task1, task2)

        val hostSpecs = mutableSetOf(createDefaultHostSpec(0), createDefaultHostSpec(1))
        val heft = HEFTPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = heft.orderTasks(input)

        Assertions.assertEquals("Task0", orderedTasks.poll().task.name)
        Assertions.assertEquals("Task1", orderedTasks.poll().task.name)
    }

    @Test
    fun whenTasksAreDependentOnOtherTasksToBeScheduledOnTwoHosts_BothTasksAreAssignedToEachHost() = runSimulation {
        val task0 = Task(UUID(0L, 1L), "Task0", HashSet(), mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1))
        val task1 = Task(UUID(0L, 2L), "Task1", setOf(task0), mutableMapOf("cpu-cycles" to 1010L, WORKFLOW_TASK_CORES to 1))
        val task2 = Task(UUID(0L, 3L), "Task2", HashSet(), mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1))
        val task3 = Task(UUID(0L, 4L), "Task3", setOf(task2), mutableMapOf("cpu-cycles" to 1000L, WORKFLOW_TASK_CORES to 1))

        val tasks = setOf(
            task0,
            task1,
            task2,
            task3)

        val cpu0 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores0 = listOf(ProcessingUnit(cpu0, 1, 3000.0))
        val cpu1 = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores1 = listOf(ProcessingUnit(cpu1, 1, 3000.0))
        val hostSpecs = mutableSetOf(
            createHostSpec(0, MachineModel(cores0, emptyList())),
            createHostSpec(1, MachineModel(cores1, emptyList())))

        val heft = HEFTPolicy(hostSpecs)
        val input = createInputForPolicy(tasks, coroutineContext)

        val orderedTasks = heft.orderTasks(input)

        val firstTask = orderedTasks.poll()
        val secondTask = orderedTasks.poll()
        val thirdTask = orderedTasks.poll()
        val fourthTask = orderedTasks.poll()
        println(firstTask.task.name + " - " + secondTask.task.name + " - " + thirdTask.task.name + " - " + fourthTask.task.name)
        Assertions.assertEquals("Task0", firstTask.task.name)
        Assertions.assertEquals("Task2", secondTask.task.name)
        Assertions.assertEquals("Task1", thirdTask.task.name)
        Assertions.assertEquals("Task3", fourthTask.task.name)
    }

    /**
     * Returns a HostSpec, which contains one CPU with one core (clock frequency = 3400)
     * and no memory units.
     */
    private fun createDefaultHostSpec(uid: Int): HostSpec {
        val cpu = ProcessingNode("PolicyMakers", "x86", "EPIC1", 1)
        val cores = listOf(ProcessingUnit(cpu, 1, 3400.0))

        val machineModel = MachineModel(cores, emptyList())
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

    private fun createInputForPolicy(tasks: Set<Task>, context: CoroutineContext): Set<TaskState> {
        val cont = Continuation<Unit>(context) { ; }
        val job = JobState(Job(UUID.randomUUID(), "onlyJob", tasks), 0, cont)
        val taskInstances = tasks.associateWith { TaskState(job, it) }

        for ((task, ts) in taskInstances) {
            ts.dependencies.addAll(task.dependencies.map { taskInstances[it]!! })
            task.dependencies.forEach {
                taskInstances[it]!!.dependents.add(ts)
            }
        }

        return taskInstances.values.toSet()
    }
}
