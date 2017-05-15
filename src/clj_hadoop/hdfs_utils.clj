(ns clj-hadoop.hdfs-utils)

(use 'clj-common.path)

(require '[clj-common.io :as io])
(require '[clj-hadoop.hdfs :as hdfs])

(defn- list-path-seq
  "Lists path and returns seq of FileStatus objects"
  [path]
  (let [fs (org.apache.hadoop.fs.FileSystem/get
             (java.net.URI/create path)
             (new org.apache.hadoop.conf.Configuration))
        path-str (path->string path)]
		(.listStatus fs (new org.apache.hadoop.fs.Path path))))

(defn delete-empty-files-in-dir [path-to-dir]
  (let [files-seq (list-path-seq path-to-dir)]
    (doseq [file (filter
                   (fn [file]
                     (= (.getLen file) 0))
                   files-seq)]
      (println "deleting " (.toString (.getPath file)))
      (if
        (not (hdfs/delete (string->path (.toString (.getPath file)))))
        (println "failed delete to trash for: " (.toString (.getPath file)))))))

(defn move-non-empty-files-to-dir [path-to-search-for path-to-move]
  (let [files-seq (list-path-seq path-to-search-for)]
    (doseq [file (filter
                   (fn [file]
                     (> (.getLen file) 0))
                   files-seq)]
      (let [src-path (string->path (.toString (.getPath file)))
            dest-path (string->path (str path-to-move "/" (.getName (.getPath file))))]
        (println "moving " src-path " to " dest-path)
        (hdfs/move src-path dest-path)))))

(defn merge-files-with-prefix-in-dir [path-to-dir prefix output-path]
  (let [files-seq (list-path-seq path-to-dir)]
    (with-open [output-stream (hdfs/output-stream output-path)]
      (doseq [file files-seq]
        (let [name (.getName (.getPath file))]
          (if (clojure.string/starts-with? name prefix)
            (with-open [input-stream (hdfs/input-stream (string->path (.toString (.getPath file))))]
              (io/copy-input-to-output-stream input-stream output-stream))))))))
