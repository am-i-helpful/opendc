package org.opendc.workflow.service

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.opendc.experiments.compute.topology.HostSpec
import org.opendc.simulator.compute.SimPsuFactories
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.power.CpuPowerModels
import org.opendc.simulator.flow2.mux.FlowMultiplexerFactory
import org.opendc.simulator.kotlin.runSimulation
import org.opendc.workflow.api.Job
import org.opendc.workflow.api.Task
import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.scheduler.task.AntColonyPolicy
import org.opendc.workflow.service.scheduler.task.Constants
import java.util.*
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext

class AntColonyPolicyTests {
    @Test
//    @Disabled
    fun example() = runSimulation {
        val hostSpecs = listOf(createDefaultHostSpec(1))

        val tasks = setOf(
            Task(UUID(0L, 1L), "Task1", emptySet(), mutableMapOf("cpu-cycles" to 1000L)),
            Task(UUID(0L, 2L), "Task2", emptySet(), mutableMapOf("cpu-cycles" to 1000L)),
            Task(UUID(0L, 3L), "Task3", emptySet(), mutableMapOf("cpu-cycles" to 1000L)),
            Task(UUID(0L, 4L), "Task4", emptySet(), mutableMapOf("cpu-cycles" to 1000L)),
            Task(UUID(0L, 5L), "Task5", emptySet(), mutableMapOf("cpu-cycles" to 1000L)),
            Task(UUID(0L, 6L), "Task6", emptySet(), mutableMapOf("cpu-cycles" to 1500L)),
            Task(UUID(0L, 7L), "Task7", emptySet(), mutableMapOf("cpu-cycles" to 1500L)),
            Task(UUID(0L, 8L), "Task8", emptySet(), mutableMapOf("cpu-cycles" to 2000L)),
            Task(UUID(0L, 9L), "Task9", emptySet(), mutableMapOf("cpu-cycles" to 2000L)),
            Task(UUID(0L, 10L), "Task10", emptySet(), mutableMapOf("cpu-cycles" to 2000L)),
            Task(UUID(0L, 11L), "Task11", emptySet(), mutableMapOf("cpu-cycles" to 2000L)),
            Task(UUID(0L, 12L), "Task12", emptySet(), mutableMapOf("cpu-cycles" to 2000L)),
            Task(UUID(0L, 13L), "Task13", emptySet(), mutableMapOf("cpu-cycles" to 2000L)),
            Task(UUID(0L, 14L), "Task14", emptySet(), mutableMapOf("cpu-cycles" to 3000L)),
            Task(UUID(0L, 15L), "Task15", emptySet(), mutableMapOf("cpu-cycles" to 3000L)),
            Task(UUID(0L, 16L), "Task16", emptySet(), mutableMapOf("cpu-cycles" to 3000L)),
            Task(UUID(0L, 17L), "Task17", emptySet(), mutableMapOf("cpu-cycles" to 3000L)),
            Task(UUID(0L, 18L), "Task18", emptySet(), mutableMapOf("cpu-cycles" to 4000L)),
            Task(UUID(0L, 19L), "Task19", emptySet(), mutableMapOf("cpu-cycles" to 7000L)),
            Task(UUID(0L, 20L), "Task20", emptySet(), mutableMapOf("cpu-cycles" to 12000L)))
        val taskStates = createInputForPolicy(tasks, coroutineContext)

        val constants = Constants(numIterations = 500, numAnts = 200, alpha = 0.9, beta = 0.1, gamma = 1.0,
            initialPheromone = 10.0, rho = 0.3)
        val policy = AntColonyPolicy(hostSpecs, constants)
        val orderedTasks = policy.orderTasks(taskStates)

        for (task in orderedTasks)
            Assertions.assertTrue(task.task.metadata.containsKey("assigned-host"))
    }

    private fun createDefaultHostSpec(uid: Int): HostSpec {
        val cpu = ProcessingNode("PolicyMakers", "x86", "EPIC1", 4)
        val cores = listOf(
            ProcessingUnit(cpu, 1, 1000.0),
            ProcessingUnit(cpu, 2, 1000.0),
            ProcessingUnit(cpu, 3, 1000.0),
            ProcessingUnit(cpu, 4, 1000.0),
            ProcessingUnit(cpu, 5, 2000.0),
            ProcessingUnit(cpu, 6, 2000.0),
            ProcessingUnit(cpu, 7, 2000.0),
            ProcessingUnit(cpu, 8, 2000.0),
            ProcessingUnit(cpu, 9, 3000.0),
            ProcessingUnit(cpu, 10, 3000.0),
            ProcessingUnit(cpu, 11, 8000.0))

        val machineModel = MachineModel(cores, emptyList<MemoryUnit>())
        return createHostSpec(uid, machineModel)
    }

    private fun createHostSpec(uid: Int, machineModel: MachineModel): HostSpec {
        return HostSpec(
            UUID(0, uid.toLong()),
            "host-$uid",
            emptyMap(),
            machineModel,
            SimPsuFactories.simple(CpuPowerModels.cubic(350.0, 200.0)),
            FlowMultiplexerFactory.forwardingMultiplexer()
        )
    }

    private fun createInputForPolicy(tasks: Set<Task>, context: CoroutineContext): List<TaskState> {
        val cont = Continuation<Unit>(context) { ; }
        val job = JobState(Job(UUID.randomUUID(), "onlyJob", tasks), 0, cont)
        return tasks.map { TaskState(job, it) }
    }

}
