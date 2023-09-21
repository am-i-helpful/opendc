package org.opendc.workflow.service.scheduler.task

import org.opendc.experiments.compute.topology.HostSpec
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import java.util.*
import kotlin.math.log10
import kotlin.math.pow
import kotlin.random.Random

internal class Core(val id: Int, val host: HostSpec, val frequency: Double) {
    private val execTimes: MutableMap<TaskState, Double> = mutableMapOf()

    // The sum of execution times of already scheduled tasks
    private var _committedTime: Double = 0.0
    val committedTime: Double
        get() { return _committedTime }

    fun getExecTime(task: TaskState): Double {
        return this.execTimes.getValue(task)
    }

    fun commitTask(task: TaskState) {
        _committedTime += this.execTimes.getValue(task)
    }

    /**
     * Clear previous information and precompute execution times for batch of tasks
     */
    fun precomputeExecTimes(tasks: List<TaskState>) {
        this.execTimes.clear()

        for (task in tasks) {
            val cpuCycles = task.task.metadata["cpu-cycles"] as Long
            val execTime = cpuCycles / frequency
            this.execTimes[task] = maxOf(execTime, 0.1)
        }
    }
}

internal class Tour() {
    private val nodes: MutableList<Pair<TaskState, Core>> = mutableListOf()
    private var makespan: Double = 0.0

    fun addNode(task: TaskState, core: Core, coreActiveTime: Double) {
        this.nodes.add(Pair(task, core))
        this.makespan = maxOf(this.makespan, coreActiveTime)
    }

    fun getMakespan() = this.makespan
    fun getNodes(): List<Pair<TaskState, Core>> = this.nodes
}

internal class Ant {
    // The sum of execution times per core for scheduled tasks
    // that are part of the current tour
    private var _coresActiveTime = mutableMapOf<Core, Double>()
    val coresActiveTime: Map<Core, Double>
        get() { return _coresActiveTime }

    private var _tour = Tour()
    val tour: Tour
        get() { return _tour }

    fun addNode(task: TaskState, core: Core) {
        val startTime = _coresActiveTime.getOrDefault(core, 0.0)
        val execTime = core.getExecTime(task)
        val coreActiveTime = startTime + execTime
        _coresActiveTime[core] = coreActiveTime

        this.tour.addNode(task, core, coreActiveTime)
    }

    fun reset() {
        _coresActiveTime.clear()
        _tour = Tour()
    }
}

// alpha and beta determine weight of trail level and task execution time
// when calculating attractiveness.
// gamma is constant I introduced to set minimum attractiveness. When >0,
// every path is considered with a certain probability >0, even if trail level
// and execution time suggest otherwise.
// rho determines the evaporation of pheromone
public data class Constants(val numIterations: Int,
                            val numAnts: Int,
                            val alpha: Double,
                            val beta: Double,
                            val gamma: Double,
                            val initialPheromone: Double,
                            val rho: Double) {}

public class AntColonyPolicy(private val hosts: List<HostSpec>, private val constants: Constants) : HolisticTaskOrderPolicy {
    private val _emptyQueue = LinkedList<TaskState>()
    private val _cores = hosts.flatMap { host -> host.model.cpus.map { Core(it.id, host, it.frequency) } }

    public override fun orderTasks(tasks: List<TaskState>): Queue<TaskState> {
        if (tasks.isEmpty())
            return _emptyQueue

        // Tasks are split into chunks of 1000 because if there are a lot of possible ProcessingUnits and a lot of tasks, an
        // OutOfMemoryException is thrown when trying to allocate the collections that hold the precalculated execution times
        // and trail levels, respectively.
        // This is also similar to the algorithm of Tawfeek et al., but with the chunkSize being equal to the number of
        // processing units.
        val chunkSize = 1000
        for (chunk in tasks.chunked(chunkSize)) {
            val goodTour = acoProc(chunk, _cores)

            for ((task, core) in goodTour.getNodes()) {
                val assignedHost = core.host
                task.task.metadata["assigned-host"] = Pair(assignedHost.uid, assignedHost.name)
                core.commitTask(task)
            }

            println("Result makespan: ${goodTour.getMakespan()}")
        }

        // Tasks are ordered FCFS
        return LinkedList(tasks)
    }

    private fun acoProc(tasks: List<TaskState>, cores: List<Core>): Tour {
        val bestTours: MutableSet<Tour> = mutableSetOf()

        val ants = initializeAnts(constants.numAnts)
        val trails = initializeTrails(constants.initialPheromone, tasks, cores)
        cores.forEach { it.precomputeExecTimes(tasks) }

        for (i in 0 until constants.numIterations) {
            ants.parallelStream().forEach { antWalk(it, tasks, cores, trails) }

            for (ant in ants) {
                val minMakespan = bestTours.firstOrNull()?.getMakespan() ?: Double.MAX_VALUE
                val currentMakespan = ant.tour.getMakespan()
                if (currentMakespan < minMakespan) {
                    bestTours.clear()
                    bestTours.add(ant.tour)
                } else if (currentMakespan == minMakespan) {
                    bestTours.add(ant.tour)
                }
            }

            val minMakespan = bestTours.first().getMakespan()
            updatePheromoneLocally(trails, ants, minMakespan)
            updatePheromoneGlobally(trails, bestTours, minMakespan)

            /*
            val taskInfos = mutableMapOf<TaskState, MutableList<String>>()
            for ((node, level) in trails) {
                taskInfos.putIfAbsent(node.first, mutableListOf())
                taskInfos.getValue(node.first).add("Core ${node.second.id}: $level")
            }
            for ((task, infos) in taskInfos) {
                println("Task ${task.task.uid.leastSignificantBits}: ${infos.joinToString()}")
            }
            */

            println("Iteration $i: Makespan ${bestTours.first().getMakespan()}")
        }

        return bestTours.random()
    }

    private fun initializeAnts(numAnts: Int) : Set<Ant> {
        return (0 until numAnts).map { Ant() }.toSet()
    }

    private fun initializeTrails(initialValue: Double, tasks: List<TaskState>, cores: List<Core>): MutableMap<Pair<TaskState, Core>, Double> {
        val trails = mutableMapOf<Pair<TaskState, Core>, Double>()
        for (task in tasks) {
            for (core in cores) {
                trails[Pair(task, core)] = initialValue
            }
        }
        return trails
    }

    // Ant walks along a tour that includes every task
    private fun antWalk(ant: Ant, tasks: List<TaskState>, cores: List<Core>, trails: Map<Pair<TaskState, Core>, Double>) {
        ant.reset()

        for (task in tasks) {
            val selectedCore = selectCore(ant, task, cores, trails)
            ant.addNode(task, selectedCore)
        }
    }

    private fun selectCore(ant: Ant, task: TaskState, cores: List<Core>, trails: Map<Pair<TaskState, Core>, Double>): Core {
        // Selects a core with certain probability. To calculate the probability, all attractivenesses have to be calculated first.
        val attractivenesses = mutableListOf<Double>()
        var attractivenessSum = 0.0
        for (i in cores.indices) {
            val core = cores[i]
            val trailLevel = trails.getValue(Pair(task, core))
            val completionTime = core.committedTime + ant.coresActiveTime.getOrDefault(core, 0.0) + core.getExecTime(task)

            val attractiveness = calculateAttractiveness(trailLevel, completionTime)
            attractivenesses.add(i, attractiveness)
            attractivenessSum += attractiveness
        }

        val toss = Random.nextDouble(attractivenessSum)
        var x = 0.0
        for (i in cores.indices) {
            x += attractivenesses[i]
            if (x > toss) {
                return cores[i]
            }
        }
        throw Exception("Toss was greater than attractivenessSum.")
    }

    private fun calculateAttractiveness(trailLevel: Double, completionTime: Double): Double {
        // Tawfeek et al. use 1.0/execTime as metric. But this would not consider the utilization of
        // cores (their availability times, to be specific), hence I use the completionTime here.
        // If the completionTime is very large, the metric becomes insignificant. Therefore, I take
        // the logarithm to flatten the curve.
        // The rest of this formula is there to get nicer values that make sense for many different traces.
        val desirability = 100.0 / maxOf(1.0, log10(completionTime)).pow(constants.beta)
        return trailLevel.pow(constants.alpha) * desirability.pow(constants.beta) + constants.gamma
    }

    private fun updatePheromoneLocally(trails: MutableMap<Pair<TaskState, Core>, Double>, ants: Set<Ant>, minMakespan: Double) {
        for ((node, oldValue) in trails.entries) {
            trails[node] = (1 - constants.rho) * oldValue
        }

        for (ant in ants) {
            for (node in ant.tour.getNodes()) {
                val pheromoneDelta = minMakespan / ant.tour.getMakespan()
                val oldValue = trails.getValue(node)
                trails[node] = oldValue + pheromoneDelta * 1.5
            }
        }
    }

    private fun updatePheromoneGlobally(trails: MutableMap<Pair<TaskState, Core>, Double>, bestTours: Set<Tour>, minMakespan: Double) {
        for (tour in bestTours) {
            for (node in tour.getNodes()) {
                val pheromoneDelta = minMakespan / tour.getMakespan()
                val oldValue = trails.getValue(node)
                trails[node] = oldValue + pheromoneDelta * 3.0
            }
        }
    }

    public override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
        //TODO("This will never be implemented, because this is just a dirty hack to get our policy running without changing doSchedule too much ¯\\_(ツ)_/¯")
        return Comparator<TaskState> { a, b ->
            when {
                a == null && b == null -> return@Comparator 0
                a == null -> return@Comparator -1
                b == null -> return@Comparator 1

                else -> return@Comparator 0
            }
        }
    }
}
