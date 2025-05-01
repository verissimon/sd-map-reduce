# Objective

Design and implement a distributed MapReduce-style processing system that uses Redis for task coordination and message passing. You will simulate a cluster of mappers and reducers that process data collaboratively in multiple stages: Map, Shuffle, and Reduce.

## Input

You are given a large text file (data.txt, around 1GB), which should be split into 10 parts (chunk0.txt to chunk9.txt) of roughly equal size (approximately 100MB). Each part contains raw text, with lines of words (e.g., sentences or phrases).

## Your Task

Implement a distributed MapReduce system, divided into three main stages:

1. Mapper Workers. Create mapper workers that:
	- Fetch a task from a Redis queue (e.g., process chunk3.txt).
	- Apply a map function (e.g., for word count, emit (word, 1) pairs).
	- Write emitted key-value pairs to an intermediate file (one per mapper) in a shared location.
2. Shuffle Phase (Coordinator Script or Service); After all mappers finish, a shuffler process should:
	- Read all intermediate files.
	- Group values by key (e.g., "hello": [1, 1, 1, 1]).
	- Partition the keys into R reducers using a hash function (e.g., hash(key) %R).
	- Write grouped key-value lists to files: reducer 0 input.json, etc.
3. Reducer workers; Create reducer workers that:
	- Read one of the reducer input files.
	- Apply a reduce function (e.g., sum all values for each key).
	- Write the result to a final output file (e.g., reducer0_output.txt).
4. Coordinator Script
	- Push all mapper tasks into the Redis queue.
	- Wait for all mappers to finish (can use pub/sub or flags).
	- Trigger the shuffle step.
	- Push reducer tasks into Redis.
	- Wait for reducers to finish.
	- Merge all reducer output files into a final result: final_result.txt.

## Requirements

- You must use Redis for queueing mapper and reducer tasks.
