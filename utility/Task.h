/*
 * Task.h
 *
 *  Created on: May 9, 2015
 *      Author: yukai
 */

#ifndef SIMPLETHREADPOOL_TASK_H_
#define SIMPLETHREADPOOL_TASK_H_

#include <unistd.h>
#include <sys/syscall.h>
#include "CpuScheduler.h"
#include "../common/Logging.h"
//#include "Logs.h"

class Task{
public:
	typedef void *(*void_function)(void *);
public:
//	Task() = default;
	Task(void_function f, void *a):func_(f),arg_(a){}
	virtual ~Task(){}
	virtual void Run() {
		(*func_)(arg_);
	}

	static inline void DestroyTask(Task *task){
		delete task;
	};

protected:
	void_function func_;	// pointer to function
	void *arg_;				// the parameter of function
};

class NumaSensitiveTask: public Task {
public:
//	NumaSensitiveTask(){};
	NumaSensitiveTask(void_function f, void *a, int node_index):
		Task(f, a), node_index_(node_index){}
	virtual ~NumaSensitiveTask() throw() {};

	virtual void Run();
	void BindSocket(int node_index);

private:
	int node_index_;
};

class DestroyTask: public Task {
public:
	DestroyTask():Task(NULL, NULL){};
	~DestroyTask() throw() {}
	void Run() {
//		Logs::log("pthread_exit...\n");
		pthread_exit(NULL);
	}
};

#endif /* SIMPLETHREADPOOL_TASK_H_ */
