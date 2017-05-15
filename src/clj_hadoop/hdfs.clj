(ns clj-hadoop.hdfs)

(use 'clj-common.path)

(def ^:dynamic *configuration* (new org.apache.hadoop.conf.Configuration))

(def ^:dynamic *hdfs-host-port* nil)

(defn create-hadoop-path [path]
  (let [path-string (str "hdfs://" *hdfs-host-port* (path2string path))]
    (new org.apache.hadoop.fs.Path path-string)))

(defn create-fs []
  (let [hdfs-uri (java.net.URI/create (str "hdfs://" *hdfs-host-port*))
        fs (org.apache.hadoop.fs.FileSystem/get hdfs-uri *configuration*)]
    fs))

(defn exists
  "Checks if path exists on hdfs"
  [path]

  (let [path-hdfs (create-hadoop-path *hdfs-host-port* path)
        fs (create-fs *hdfs-host-port*)]
    (.exists fs path-hdfs)))

(defn mkdirs
  "Ensures given path exists, making all non existing dirs"
  [path]

  (let [path-hdfs (create-hadoop-path *hdfs-host-port* path)
        fs (create-fs *hdfs-host-port*)]
    (.mkdirs fs path-hdfs)))

(defn move
  "Moves hdfs path to new path"
  [source-path destination-path]
  (let [source-path-hdfs (create-hadoop-path *hdfs-host-port* source-path)
        destination-path-hdfs (create-hadoop-path *hdfs-host-port* destination-path)
        fs (create-fs *hdfs-host-port*)]
    (.rename fs source-path-hdfs destination-path-hdfs)))

(defn move-to-trash [path]
  (let [path-str (path->string path)
        fs (org.apache.hadoop.fs.FileSystem/get (java.net.URI/create path-str) *configuration*)]
    (let [trash (new org.apache.hadoop.fs.Trash fs *configuration*)]
      (.moveToTrash trash (new org.apache.hadoop.fs.Path path-str)))))

(defn delete
  [path]
  (let [fs (create-fs *hdfs-host-port*)]
    (.delete fs (new org.apache.hadoop.fs.Path (path->string path)))))

(defn is-directory
  "Checks if given path represents directory"
  [path]

  (let [path-hdfs (create-hadoop-path *hdfs-host-port* path)
        fs (create-fs *hdfs-host-port*)
        file-status (.getFileStatus fs path-hdfs)]
    (.isDir file-status)))

(defn list
  "List paths on given path if directory, if file or doesn't exist empty list is returned"
  [path]

  (let [path-hdfs (create-hadoop-path *hdfs-host-port* path)
        fs (create-fs *hdfs-host-port*)
        file-statuses (.listStatus fs path-hdfs)]

    (map
      (partial child path)
      (map
        (fn [fileStatus] (.getName (.getPath fileStatus)))
        file-statuses))))

(defn input-stream
  "Creates input stream for given path"
  [path]

  (let [path-hdfs (create-hadoop-path *hdfs-host-port* path)
        fs (create-fs *hdfs-host-port*)]
    (.open fs path-hdfs)))

(defn output-stream
  "Creates output stream for given path"
  [path]

  (let [path-hdfs (create-hadoop-path *hdfs-host-port* path)
        fs (create-fs *hdfs-host-port*)]
    (.create fs path-hdfs)))

(defn output-stream-by-appending
  "Creates output stream for given path by appending"
  [path]
  (throw (new java.lang.UnsupportedOperationException "appending is not supported")))


