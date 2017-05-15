(ns clj-hadoop.mapreduce.job)

(require '[clj-common.hdfs :as hdfs])
(require '[clj-common.path :as path])


(defn create-job [job-name]
  (let [configuration hdfs/*configuration*]
    (new org.apache.hadoop.mapreduce.Job configuration job-name)))

(defn set-map-only [job]
  (.set (.getConfiguration job) "mapred.reduce.tasks" "0")
  job)

(defn set-reducer-number [job number]
  (.set (.getConfiguration job) "mapred.reduce.tasks" (str number))
  job)

(defn set-jar-to-job-class [job]
  (.setJarByClass job org.apache.hadoop.mapreduce.Job)
  job)

(defn set-text-output [job]
  (.setOutputKeyClass job org.apache.hadoop.io.Text)
  (.setOutputValueClass job org.apache.hadoop.io.Text)
  (org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat/setOutputFormatClass
    job
    org.apache.hadoop.mapreduce.lib.output.TextOutputFormat)
  job)

(defn set-mapper [job mapper-class]
  (.setMapperClass job mapper-class)
  job)

(defn set-reducer [job reducer-class]
  (.setReducerClass job reducer-class)
  job)

(defn set-input-paths [job hadoop-host-port paths]
  (doseq [path paths]
    (let [hdfs-path (new
                      org.apache.hadoop.fs.Path
                      (str
                        "hdfs://"
                        hadoop-host-port
                        (path/path2string path)))]
      (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath
        job
        hdfs-path)))
  job)

(defn set-output-path [job hadoop-host-port path]
  (let [hdfs-path (new
                      org.apache.hadoop.fs.Path
                      (str
                        "hdfs://"
                        hadoop-host-port
                        (path/path2string path)))]
    (org.apache.hadoop.mapreduce.lib.output.FileOutputFormat/setOutputPath
      job
      hdfs-path))
  job)

(defn set-slow-reduce-start [job]
  (.set (.getConfiguration job) "mapred.reduce.slowstart.completed.maps" "1.0")
  job)

(defn set-in-configuration [job key value]
  (.set (.getConfiguration job) key (str value)))

(defn submit-and-wait [job]
  (let [result (.waitForCompletion job true)]
    (if (not result)
      (throw (new Exception "Job failed")))))
