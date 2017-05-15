(ns clj-hadoop.mapreduce.utils)

(defn calculate-hash-partition [id, numTasks]
	(let [partitioner (new org.apache.hadoop.mapreduce.lib.partition.HashPartitioner)]
		(.getPartition partitioner (new org.apache.hadoop.io.Text (str "" id)) nil numTasks)))
