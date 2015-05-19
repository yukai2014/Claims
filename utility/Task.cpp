/*
 * Task.cpp
 *
 *  Created on: May 9, 2015
 *      Author: yukai
 */


#include "Task.h"

#define SPECIFY_CPU

/*
 *  get old binded CPU and rebind new CPU in specify socket,
 *  run function f,
 *  bind to old CPU again
 */
void NumaSensitiveTask::Run() {
	int old_cpu = getCurrentCpuAffility();
//	Logs::log("Before bind socket:\tCurrent cpu is: %d\tCurrent node is: %d\n", getCurrentCpuAffility(), getCurrentSocketAffility());
#ifdef SPECIFY_CPU
	int cpu_index = GetNextCPUinSocket(node_index_);

//	// for debug
//	if (9 == cpu_index) {
//		int
//	}


	setCpuAffility(cpu_index);
#else
	struct bitmask* bm = numa_allocate_nodemask();
	numa_bitmask_clearall(bm);
	numa_bitmask_setbit(bm, node_index_);
//	numa_bind(bm);
//	numa_run_on_node(node_index_);	// bind to the first CPU in this numa node
//	Logs::log("After numa_run_on_ndoe");

	numa_run_on_node_mask_all(bm);	// bind to the first CPU in this numa node, same as numa_run_on_node()
//	Logs::log("After numa_run_on_node_mask_all");
#endif
	ThreadPoolLogging::log("thread (tid=%ld offset=%lx) bind cpu=%ld (start=%ld end=%ld), in node %d\n",
		syscall(__NR_gettid), pthread_self(), getCurrentCpuAffility(), 0, getNumberOfCpus(), getCurrentSocketAffility());
	(*func_)(arg_);

	// restore old binding
	setCpuAffility(old_cpu);
	ThreadPoolLogging::log("After restore:\t thread (tid=%ld offset=%lx) bind cpu=%ld (start=%ld end=%ld), in node %d\n",
		syscall(__NR_gettid), pthread_self(), getCurrentCpuAffility(), 0, getNumberOfCpus(), getCurrentSocketAffility());

}

void NumaSensitiveTask::BindSocket(int node_index) {

}


