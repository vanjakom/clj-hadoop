(defproject com.mungolab/clj-hadoop "0.1.0-SNAPSHOT"
  :description "common hadoop functions"
  :url "https://github.com/vanjakom/clj-hadoop"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                  [org.clojure/clojure "1.8.0"]
                  [lein-light-nrepl "0.3.2"]

                  [com.mungolab/clj-common "0.2.0-SNAPSHOT"]

                  [
                    org.apache.hadoop/hadoop-core
                    "0.20.2-cdh3u5"]

                    ; removing exclusions since m/r job submit is failing
                    ;:exclusions [
                    ;              org.codehaus.jackson/jackson-xc
                    ;              org.codehaus.jackson/jackson-jaxrs
                    ;              org.codehaus.jackson/jackson-core-asl
                    ;              org.codehaus.jackson/jackson-mapper-asl]]

                  ;[org.apache.hadoop/hadoop-client "2.6.0-cdh5.9.1"]

                  ;[org.apache.hadoop/hadoop-client "2.7.3"]
                  ;[org.apache.hadoop/hadoop-hdfs "2.7.3"]
                  ;[org.apache.hadoop/hadoop-mapreduce-client-core "2.7.3"]
                  ]
  :repl-options {
                  :nrepl-middleware [lighttable.nrepl.handler/lighttable-ops]}
  :repositories [
                  ["cloudera" "https://repository.cloudera.com/content/repositories/releases/"]])
