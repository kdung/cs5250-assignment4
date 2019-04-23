"""
CS5250 Assignment 4, Scheduling policies simulator
Sample skeleton program
Input file:
    input.txt
Output files:
    FCFS.txt
    RR.txt
    SRTF.txt
    SJF.txt
"""

import sys
from collections import deque
import heapq

input_file = 'input.txt'


class Process:
    last_scheduled_time = 0

    def __init__(self, id, arrive_time, burst_time):
        self.id = id
        self.arrive_time = arrive_time
        self.burst_time = burst_time
        self.predicted_time = 5
        self.remained_time = burst_time
    # for printing purpose

    def __repr__(self):
        return '[id %d : arrival_time %d,  burst_time %d]'%(self.id, self.arrive_time, self.burst_time)

    def __lt__(self, other):
        return self.arrive_time < other.arrive_time


def FCFS_scheduling(process_list):
    # store the (switching time, process_id) pair
    schedule = []
    current_time = 0
    waiting_time = 0
    for process in process_list:
        if current_time < process.arrive_time:
            current_time = process.arrive_time
        schedule.append((current_time, process.id))
        waiting_time = waiting_time + (current_time - process.arrive_time)
        current_time = current_time + process.burst_time
    average_waiting_time = waiting_time/float(len(process_list))
    return schedule, average_waiting_time


# Input: process_list, time_quantum (Positive Integer)
# Output_1 : Schedule list contains pairs of (time_stamp, process_id) indicating the time switching to that proccess_id
# Output_2 : Average Waiting Time
def RR_scheduling(process_list, time_quantum ):
    schedule = []
    current_time = 0
    waiting_time = 0
    ready_queue = deque()
    total_process = len(process_list)
    blocked_queue = []
    suspend_queue = deque(process_list)
    while suspend_queue or ready_queue or blocked_queue:
        # getting new process
        taken_processes = []
        for waiting_process in suspend_queue:
            if (not ready_queue and not blocked_queue) or current_time >= waiting_process.arrive_time:
                ready_queue.append(waiting_process)
                taken_processes.append(waiting_process)
            else:
                break
        # remove processes which have been moved to ready
        for process in taken_processes:
            suspend_queue.remove(process)
        # execute ready process

        if blocked_queue:
            ready_queue.extend(blocked_queue)
            blocked_queue.clear()
        # print(ready_queue)
        if ready_queue:
            process = ready_queue.popleft()
            if current_time < process.arrive_time:
                current_time = process.arrive_time
            schedule.append((current_time, process.id))
            if process.remained_time <= time_quantum:
                current_time = current_time + process.remained_time
                waiting_time = waiting_time + (current_time - process.arrive_time - process.burst_time)
            else:
                current_time = current_time + time_quantum
                process.remained_time = process.remained_time - time_quantum
                blocked_queue.append(process)
    average_waiting_time = waiting_time / float(total_process)
    return schedule, average_waiting_time


# scheduling process_list on SRTF, using process.burst_time
# to calculate the remaining time of the current process
def SRTF_scheduling(process_list):
    schedule = []
    current_time = 0
    waiting_time = 0
    total_process = len(process_list)
    ready_queue = []

    while ready_queue or process_list:
        if not ready_queue:
            heapq.heappush(ready_queue, (process_list[0].burst_time, process_list[0]))
            process_list.remove(process_list[0])
        running_burst_time, running_process = heapq.heappop(ready_queue)
        if current_time < running_process.arrive_time:
            current_time = running_process.arrive_time
        schedule.append((current_time, running_process.id))
        # getting new process
        taken_processes = []
        for waiting_process in process_list:
            running_process_remain_time = running_burst_time - (waiting_process.arrive_time - current_time)
            if running_process_remain_time > waiting_process.burst_time:
                # preempt current running process and pick the new arrival process
                current_time = waiting_process.arrive_time
                heapq.heappush(ready_queue, (running_process_remain_time, running_process))
                running_process = waiting_process
                running_burst_time = running_process.burst_time
                schedule.append((current_time, running_process.id))
                taken_processes.append(waiting_process)
            elif running_process_remain_time > 0:
                # update current time and remain burst time then continue
                current_time = waiting_process.arrive_time
                running_burst_time = running_process_remain_time
                heapq.heappush(ready_queue, (waiting_process.burst_time, waiting_process))
                taken_processes.append(waiting_process)
            else:
                # finishing current running process and pick new process from ready queue
                current_time = current_time + running_burst_time
                waiting_time = waiting_time + (current_time - running_process.arrive_time - running_process.burst_time)

                if waiting_process.arrive_time <= current_time:
                    heapq.heappush(ready_queue, (waiting_process.burst_time, waiting_process))
                    taken_processes.append(waiting_process)
                    running_burst_time, running_process = heapq.heappop(ready_queue)
                    if current_time < running_process.arrive_time:
                        current_time = running_process.arrive_time
                    schedule.append((current_time, running_process.id))
                else:
                    break

        # remove processes which have been moved to ready
        for process in taken_processes:
            process_list.remove(process)

        if not process_list:
            # if there is no more arriving process, finish the current running process
            current_time = current_time + running_burst_time
            waiting_time = waiting_time + (current_time - running_process.arrive_time - running_process.burst_time)

    average_waiting_time = waiting_time / float(total_process)
    return schedule, average_waiting_time


# scheduling SJF without using information from process.burst_time
def SJF_scheduling(process_list, alpha):
    schedule = []
    current_time = 0
    waiting_time = 0
    total_process = len(process_list)
    ready_queue = []
    process_hist = dict()
    while ready_queue or process_list:
        if not ready_queue:
            heapq.heappush(ready_queue, (process_list[0].predicted_time, process_list[0]))
            process_list.remove(process_list[0])
        running_predicted_time, running_process = heapq.heappop(ready_queue)
        if current_time < running_process.arrive_time:
            current_time = running_process.arrive_time
        schedule.append((current_time, running_process.id))
        waiting_time = waiting_time + (current_time - running_process.arrive_time)
        current_time = current_time + running_process.burst_time

        process_hist[running_process.id] = running_process

        # getting new processes to the ready queue
        taken_processes = []
        for waiting_process in process_list:
            if waiting_process.arrive_time <= current_time:
                # calculate predicted time for waiting_process and push to ready_queue
                if waiting_process.id in process_hist:
                    prev_instance = process_hist[waiting_process.id]
                    waiting_process.predicted_time =\
                        alpha * prev_instance.burst_time + (1 - alpha) * prev_instance.predicted_time
                heapq.heappush(ready_queue, (waiting_process.predicted_time, waiting_process))
                taken_processes.append(waiting_process)
            else:
                break

        # remove processes which have been moved to ready
        for process in taken_processes:
            process_list.remove(process)

    average_waiting_time = waiting_time / float(total_process)
    return schedule, average_waiting_time


def read_input():
    result = []
    with open(input_file) as f:
        for line in f:
            array = line.split()
            if len(array) != 3:
                print ("wrong input format")
                exit()
            result.append(Process(int(array[0]), int(array[1]), int(array[2])))
    return result


def write_output(file_name, schedule, avg_waiting_time):
    with open(file_name, 'w') as f:
        for item in schedule:
            f.write(str(item) + '\n')
        f.write('average waiting time %.2f \n' % avg_waiting_time)


def main(argv):
    process_list = read_input()
    print("printing input ----")
    for process in process_list:
        print(process)
    print("simulating FCFS ----")
    FCFS_schedule, FCFS_avg_waiting_time =  FCFS_scheduling(process_list)
    write_output('FCFS.txt', FCFS_schedule, FCFS_avg_waiting_time)
    print("simulating RR ----")
    RR_schedule, RR_avg_waiting_time =  RR_scheduling(process_list, time_quantum=2)
    write_output('RR.txt', RR_schedule, RR_avg_waiting_time)
    print("simulating SRTF ----")
    process_list = read_input()
    SRTF_schedule, SRTF_avg_waiting_time =  SRTF_scheduling(process_list)
    write_output('SRTF.txt', SRTF_schedule, SRTF_avg_waiting_time)
    print("simulating SJF ----")
    process_list = read_input()
    SJF_schedule, SJF_avg_waiting_time =  SJF_scheduling(process_list, alpha=0.5)
    write_output('SJF.txt', SJF_schedule, SJF_avg_waiting_time)


if __name__ == '__main__':
    main(sys.argv[1:])

