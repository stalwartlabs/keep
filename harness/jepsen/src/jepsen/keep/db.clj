;; SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
;;
;; SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL

(ns jepsen.keep.db
  "Installs and runs keep on each node: bootstrap on the first start, rejoin with a higher incarnation after."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]))

(def dir "/opt/keep")
(def binary (str dir "/keep"))
(def logfile (str dir "/keep.log"))
(def pidfile (str dir "/keep.pid"))
(def tls-dir (str dir "/tls"))
(def client-port 7000)
(def peer-port 7010)
(def secret "00112233445566778899aabbccddeeff")
(def peer-secret "ffeeddccbbaa99887766554433221100")

(defn node-id [test node]
  (inc (.indexOf ^java.util.List (vec (:nodes test)) node)))

(defn members [test]
  (->> (:nodes test)
       (map (fn [node]
              (str (node-id test node) "@" (cn/ip node) ":" client-port "/" peer-port "/" (:shards test))))
       (str/join ",")))

(defn client-url [test]
  (str "keep://"
       (->> (:nodes test) (map #(str (cn/ip %) ":" client-port)) (str/join ","))
       "?cluster=on&secret=" secret
       "&timeout=ms:500&deadline=ms:500&connect_timeout=ms:500"))

(defn- tls-args
  "rpk when the test has a --tls-dir. Plaintext otherwise: --tls off for builds that have the flag (aea9be4 and later, which default to rpk after the flip); older pinned builds run plaintext without it."
  [test node]
  (cond
    (:tls-dir test) [:--tls "rpk"
                     :--tls-key (str tls-dir "/n" (node-id test node) ".key")
                     :--tls-clients (str tls-dir "/clients.pem")
                     :--tls-servers (str tls-dir "/servers.pem")]
    (str/includes? (c/exec binary :--help) "--tls ") [:--tls "off"]))

(defn- upload-tls!
  "Uploads this node's key and both public key lists from the control node's --tls-dir."
  [test node]
  (when-let [local (:tls-dir test)]
    (c/exec :rm :-rf tls-dir)
    (c/exec :mkdir :-p tls-dir)
    (doseq [file [(str "n" (node-id test node) ".key") "servers.pem" "clients.pem"]]
      (c/upload (str local "/" file) (str tls-dir "/" file)))
    (c/exec :chmod :600 (str tls-dir "/n" (node-id test node) ".key"))))

(defn start! [test node mode]
  (let [ip (cn/ip node)
        incarnation (quot (System/currentTimeMillis) 1000)]
    (info node "starting keep" mode "incarnation" incarnation)
    (c/su
      (apply cu/start-daemon!
             {:logfile logfile :pidfile pidfile :chdir dir
              :env {:KEEP_SECRET secret :KEEP_PEER_SECRET peer-secret
                    :GLIBC_TUNABLES "glibc.malloc.hugetlb=0"}}
             binary
             :--listen (str ip ":" client-port) :--advertise (str ip ":" client-port)
             :--node (node-id test node) :--members (members test)
             :--shards (:shards test) :--pin "never"
             :--max-lock-ttl (:max-lock-ttl test) :--stats-interval 1
             :--start (name mode) :--incarnation incarnation
             (concat (tls-args test node)
                     (when-let [memory (:memory test)] [:--memory memory]))))))

(defn signal!
  "Signals the keep process named by the pidfile; jepsen's cu/signal! runs pkill without -f, which never matches it."
  [sig]
  (when (cu/daemon-running? pidfile)
    (let [pid (str/trim (c/exec :cat pidfile))]
      (c/exec :kill (str "-" (name sig)) pid)
      pid)))

(defn log-lines []
  (parse-long (str/trim (c/exec :wc :-l :< logfile))))

(defn pause!
  "SIGSTOP, then checks that the process is stopped and that its once-per-second control lines stop."
  []
  (if-let [pid (signal! :STOP)]
    (let [stat (str/trim (c/exec :ps :-o "stat=" :-p pid))
          before (log-lines)
          _ (Thread/sleep 2500)
          after (log-lines)]
      (when-not (and (str/starts-with? stat "T") (= before after))
        (throw (ex-info "pause did not stop keep" {:pid pid :stat stat :log-lines [before after]})))
      :paused)
    :not-running))

(defn resume!
  "SIGCONT, then checks that the process left the stopped state."
  []
  (if-let [pid (signal! :CONT)]
    (let [stat (str/trim (c/exec :ps :-o "stat=" :-p pid))]
      (when (str/starts-with? stat "T")
        (throw (ex-info "resume did not continue keep" {:pid pid :stat stat})))
      :resumed)
    :not-running))

(defn db []
  (let [started (atom #{})]
    (reify
      db/DB
      (setup! [_ test node]
        (c/su
          (c/exec :mkdir :-p dir)
          (c/upload (:keep-bin test) binary)
          (c/exec :chmod :+x binary)
          (upload-tls! test node))
        (start! test node :bootstrap)
        (swap! started conj node)
        (Thread/sleep 3000)
        (c/su
          (when-not (cu/daemon-running? pidfile)
            (throw (ex-info "keep exited at startup" {:node node :log (c/exec :tail :-n 20 logfile)})))))
      (teardown! [_ test node]
        (c/su
          (cu/stop-daemon! binary pidfile)
          (c/exec :rm :-rf logfile pidfile tls-dir))
        (swap! started disj node))

      db/LogFiles
      (log-files [_ test node] [logfile])

      db/Kill
      (start! [_ test node]
        (let [result (start! test node :rejoin)]
          (Thread/sleep 500)
          (if (c/su (cu/daemon-running? pidfile)) result :exited)))
      (kill! [_ test node] (c/su (cu/stop-daemon! binary pidfile)))

      db/Pause
      (pause! [_ test node] (c/su (pause!)))
      (resume! [_ test node] (c/su (resume!))))))

(defn terminate!
  "SIGTERM, which hands the slots over, then waits up to wait-s seconds for the exit before kill -9."
  [wait-s]
  (c/su
    (signal! :TERM)
    (loop [i 0]
      (when (and (< i wait-s) (cu/daemon-running? pidfile))
        (Thread/sleep 1000)
        (recur (inc i))))
    (cu/stop-daemon! binary pidfile)))

(defn leave!
  "Graceful leave: SIGTERM hands the slots over, then the node restarts with rejoin."
  [test node]
  (c/on node
    (terminate! 30)
    (start! test node :rejoin)))
