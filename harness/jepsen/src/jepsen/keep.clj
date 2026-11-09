;; SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
;;
;; SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL

(ns jepsen.keep
  "Jepsen test for Keep: workloads register (Knossos), counter, lock and lease (keep-jepsen check-locks, claim L), register-failover (K3 read validity) and readback (the fault suite's read-back probe under the heal-restart nemesis)."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [net :as net]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.net.proto :as p]
            [jepsen.keep.os :as kos]
            [jepsen.keep.client :as kc]
            [jepsen.keep.db :as kdb]
            [jepsen.keep.net :as kn]
            [jepsen.keep.readback :as rb]
            [knossos.model :as model]))

(def recovery-keys
  "First register key of the recovery phase, above any key the fault phase reaches, so Knossos never searches recovery ops against crashed fault-phase writes."
  1000000)

(defn- register-generator [keys]
  (independent/concurrent-generator
    5 keys
    (fn [_]
      (->> (gen/mix [(fn [] {:f :read :value nil})
                     (fn [] {:f :write :value (rand-int 5)})])
           (gen/limit 300)))))

(defn register-workload [opts]
  {:client (kc/proxy-client
             (fn [op]
               (let [[k v] (:value op)]
                 (case (:f op)
                   :read {:f "read" :key k}
                   :write {:f "write" :key k :value v})))
             (fn [op value]
               (let [[k _] (:value op)]
                 (independent/tuple k (if (= :read (:f op)) value (second (:value op)))))))
   :generator (register-generator (range))
   :recovery-generator (register-generator (range recovery-keys Long/MAX_VALUE))
   :checker (independent/checker
              (checker/linearizable {:model (model/register) :algorithm :linear}))})

(defn read-validity
  "K3: every ok read returns a value some write of that key produced, or absent."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [ops (filter #(#{:invoke :ok :info} (:type %)) history)
            written (->> ops
                         (filter #(and (= :write (:f %)) (= :invoke (:type %))))
                         (reduce (fn [m op] (let [[k v] (:value op)] (update m k (fnil conj #{}) v))) {}))
            bad (->> ops
                     (filter #(and (= :read (:f %)) (= :ok (:type %))))
                     (remove (fn [op] (let [[k v] (:value op)] (or (nil? v) (contains? (get written k) v))))))]
        {:valid? (empty? bad) :invented (take 10 bad)}))))

(defn register-failover-workload [opts]
  (assoc (register-workload opts) :checker (read-validity)))

(defn counter-workload [opts]
  {:client (kc/proxy-client
             (fn [op] (case (:f op)
                        :add {:f "incr" :key 0 :delta (:value op)}
                        :read {:f "incr" :key 0 :delta 0}))
             (fn [op value] (if (= :read (:f op)) value (:value op))))
   :generator (gen/mix [(fn [] {:f :add :value (inc (rand-int 3))})
                        (fn [] {:f :read})])
   :checker (checker/counter)})

(defn- lock-call [conn op value request]
  (try
    (let [reply (kc/call conn request)]
      (assoc op :value value
                :type (case (:type reply) "ok" :ok "fail" :fail :info)
                :error (:error reply)
                :server (:server reply)))
    (catch java.net.SocketTimeoutException _
      (assoc op :type :info :value value :error :timeout))))

(defrecord LockClient [conn tokens held]
  client/Client
  (open! [this test node]
    (assoc this :conn (kc/connect "127.0.0.1" (:proxy-port test)) :held (atom nil)))
  (setup! [_ test])
  (invoke! [_ test op]
    (case (:f op)
      :acquire (if (and (:exclusive-acquire test) @held)
                 (assoc op :type :fail :error :already-held)
                 (let [[k ttl] (:value op)
                       token (+ (* 1000000 (:process op)) (swap! tokens inc))
                       done (lock-call conn op [k ttl token] {:f "lock" :key k :token token :ttl ttl})]
                   (when (not= :fail (:type done)) (reset! held [k token]))
                   done))
      :renew (if-let [[k token] @held]
               (let [[_ ttl] (:value op)
                     done (lock-call conn op [k ttl token] {:f "renew" :key k :token token :ttl ttl})]
                 (when (= "lost" (:error done)) (reset! held nil))
                 done)
               (assoc op :type :fail :error :not-held))
      :release (if-let [[k token] @held]
                 (let [done (lock-call conn op [k nil token] {:f "unlock" :key k :token token})]
                   (when (or (= :ok (:type done)) (= "not held" (:error done))) (reset! held nil))
                   done)
                 (assoc op :type :fail :error :not-held))))
  (teardown! [_ test])
  (close! [_ test]
    (when conn (.close ^java.net.Socket (:socket conn)))))

(defn lock-client []
  (map->LockClient {:tokens (atom 0)}))

(defn- lock-outcome [op]
  (case (:type op)
    :ok "ok"
    :info "unknown"
    :fail (case (:error op) "held" "conflict" ("not held" "lost") "not_found" "busy")))

(defn lock-history-jsonl
  "Writes the lock history in the keep-jepsen history format for check-locks."
  [history file]
  (with-open [w (io/writer file)]
    (loop [ops (seq history) pending {}]
      (when-let [[op & more] ops]
        (cond
          (not (integer? (:process op))) (recur more pending)
          (= :invoke (:type op)) (recur more (assoc pending (:process op) op))
          (#{:ok :fail :info} (:type op))
          (let [invoke (get pending (:process op))
                [k ttl token] (:value op)]
            (when (and invoke token (#{:acquire :renew :release} (:f op)))
              (.write w (json/generate-string
                          (cond-> {:client (:process op)
                                   :op (case (:f op) :acquire "try_lock" :renew "renew" :release "unlock")
                                   :key (format "0e%016x" k)
                                   :token (format "%016x" token)
                                   :send_ns (:time invoke)
                                   :outcome (lock-outcome op)}
                            (#{:acquire :renew} (:f op)) (assoc :ttl_s ttl)
                            (:server op) (assoc :server (:server op))
                            (not= :info (:type op)) (assoc :reply_ns (:time op)))))
              (.write w "\n"))
            (recur more (dissoc pending (:process op))))
          :else (recur more pending))))))

(defn lock-checker []
  (reify checker/Checker
    (check [_ test history opts]
      (let [file (str (store/path test "locks.jsonl"))]
        (lock-history-jsonl history file)
        (let [{:keys [exit out err]} (sh (:keep-jepsen test) "check-locks" file)]
          {:valid? (zero? exit) :keep-check (str/trim (str out err))})))))

(defn lock-workload [opts]
  {:client (lock-client)
   :generator (->> (gen/flip-flop
                     (fn [] {:f :acquire :value [(rand-int 4) (inc (rand-int (:max-lock-ttl opts)))]})
                     (fn [] {:f :release :value [nil nil]}))
                   (gen/stagger 1/20))
   :checker (lock-checker)})

(defn lease-workload
  "Stalwart's try_lease, renew_lease and release_lease: each client acquires only while it holds nothing, renews its hold with a fresh TTL, and releases it."
  [opts]
  (let [ttl (fn [] (inc (rand-int (:max-lock-ttl opts))))]
    {:client (lock-client)
     :exclusive-acquire true
     :generator (->> (gen/mix [(fn [] {:f :acquire :value [(rand-int 4) (ttl)]})
                               (fn [] {:f :renew :value [nil (ttl)]})
                               (fn [] {:f :renew :value [nil (ttl)]})
                               (fn [] {:f :release :value [nil nil]})])
                     (gen/stagger 1/20))
     :checker (lock-checker)}))

(def workloads
  {:register register-workload
   :register-failover register-failover-workload
   :counter counter-workload
   :lock lock-workload
   :lease lease-workload
   :readback (fn [_] (rb/workload (atom -1)))})

(defn leave-nemesis []
  (reify
    nemesis/Reflection
    (fs [_] #{:leave})
    nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [_ test op]
      (let [node (rand-nth (:nodes test))]
        (kdb/leave! test node)
        (assoc op :value node)))
    (teardown! [_ test])))

(def packet-behaviors
  [{:delay {}}
   {:delay {:time :200ms :jitter :100ms}}
   {:loss {:percent :10%}}
   {:loss {:percent :40%}}
   {:duplicate {:percent :25%}}
   {:reorder {:percent :25%}}
   {:corrupt {:percent :5%}}
   {:rate {:rate :1mbit}}
   {:delay {} :loss {:percent :5%} :duplicate {:percent :10%} :reorder {:percent :10%} :corrupt {:percent :1%}}])

(defn one-way-grudge
  "Cuts a random minority one way: either it hears nobody (inward) or nobody hears it (outward); TCP fails both ways, UDP heartbeats only one way."
  [nodes]
  (let [[minority majority] (split-at (inc (rand-int (quot (dec (count nodes)) 2))) (shuffle nodes))
        inward (rand-nth [true false])]
    (if inward
      (zipmap minority (repeat (set majority)))
      (zipmap majority (repeat (set minority))))))

(def cut-grudges
  {:bridge (fn [nodes] (nemesis/bridge (shuffle nodes)))
   :one-way one-way-grudge})

(defn cut-nemesis
  "Partitions the combined package lacks: :start-cut takes a grudge kind from cut-grudges, :stop-cut heals. Its rules live in their own chain."
  []
  (let [cut-net (kn/chain-net "keep-cut")]
    (reify
      nemesis/Reflection
      (fs [_] #{:start-cut :stop-cut})
      nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [_ test op]
        (case (:f op)
          :start-cut (let [grudge ((cut-grudges (:value op)) (:nodes test))]
                       (p/drop-all! cut-net test grudge)
                       (assoc op :value [(:value op) grudge]))
          :stop-cut (do (net/heal! cut-net test)
                        (assoc op :value :network-healed))))
      (teardown! [_ test]
        (net/heal! cut-net test)))))

(defn cut-package [opts kinds]
  {:nemesis (cut-nemesis)
   :generator (gen/cycle [(gen/sleep (:nemesis-interval opts))
                          (gen/once (fn [] {:type :info :f :start-cut :value (rand-nth kinds)}))
                          (gen/sleep (:nemesis-interval opts))
                          {:type :info :f :stop-cut}])
   :final-generator {:type :info :f :stop-cut}
   :perf #{}})

(defn nemesis-package [opts db workload]
  (if (= [:heal-restart] (:nemesis opts))
    (rb/package opts (:next-key workload))
    (let [faults (set (:nemesis opts))
          node-targets (if (:wide-targets opts) [:one :minority :majority :all] [:one :minority])
          cut-kinds (vec (filter faults (keys cut-grudges)))
          combined (nc/nemesis-package {:db db :nodes (:nodes opts)
                                        :faults (apply disj faults :leave (keys cut-grudges))
                                        :interval (:nemesis-interval opts)
                                        :partition {:targets [:one :majority :majorities-ring :minority-third]}
                                        :packet {:targets [:one :minority :majority :all]
                                                 :behaviors packet-behaviors}
                                        :kill {:targets node-targets}
                                        :pause {:targets node-targets}})]
      (nc/compose-packages
        (cond-> [combined]
          (seq cut-kinds) (conj (cut-package opts cut-kinds))
          (faults :leave) (conj {:nemesis (nemesis/f-map {:leave :leave} (leave-nemesis))
                                 :generator (gen/cycle [(gen/sleep (:nemesis-interval opts)) {:type :info :f :leave}])
                                 :final-generator nil
                                 :perf #{}}))))))

(defn recovery-checker
  "Liveness after the faults: some client op completes ok after the last nemesis op, within the recovery phase."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [healed (->> history (filter #(= :nemesis (:process %))) (map :time) (reduce max 0))
            first-ok (->> history
                          (filter #(and (integer? (:process %)) (= :ok (:type %)) (> (:time %) healed)))
                          (map :time)
                          first)]
        {:valid? (some? first-ok)
         :first-ok-after-s (when first-ok (/ (- first-ok healed) 1e9))}))))

(defn- check-faults! [opts]
  (let [faults (:nemesis opts)]
    (when (and (some #{:heal-restart} faults) (not= [:heal-restart] faults))
      (throw (IllegalArgumentException. "heal-restart runs alone: another fault could move or lose a judged write")))
    (when (not= (= :readback (:workload opts)) (= [:heal-restart] faults))
      (throw (IllegalArgumentException. "the readback workload and the heal-restart nemesis run only together")))))

(defn keep-test [opts]
  (check-faults! opts)
  (let [db (kdb/db)
        workload ((workloads (:workload opts)) opts)
        package (nemesis-package opts db workload)]
    (merge tests/noop-test
           opts
           {:name (str "keep " (name (:workload opts)) " " (str/join "," (map name (:nemesis opts)))
                       (when (kos/stop-time-sync? (assoc opts :faults (:nemesis opts))) " ntp-off"))
            :faults (:nemesis opts)
            :os kos/os
            :net (kn/chain-net "keep-part")
            :db db
            :client (:client workload)
            :exclusive-acquire (:exclusive-acquire workload)
            :nemesis (:nemesis package)
            :checker (checker/compose {:workload (:checker workload)
                                       :recovery (recovery-checker)
                                       :stats (checker/stats)
                                       :exceptions (checker/unhandled-exceptions)})
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis (:generator package))
                              (gen/time-limit (:time-limit opts)))
                         (gen/nemesis (:final-generator package))
                         (gen/sleep 10)
                         (->> (or (:recovery-generator workload) (:generator workload))
                              (gen/stagger (/ (:rate opts)))
                              (gen/clients)
                              (gen/time-limit (:recovery opts))))})))

(def cli-opts
  [[nil "--workload NAME" "register, register-failover, counter, lock, lease or readback (with --nemesis heal-restart)"
    :default :lock :parse-fn keyword :validate [workloads "unknown workload"]]
   [nil "--nemesis FAULTS" "comma-separated: kill, pause, partition, bridge, one-way, packet, clock, leave; or heal-restart alone"
    :default [] :parse-fn #(mapv keyword (remove str/blank? (str/split % #",")))]
   [nil "--nemesis-interval SECS" :default 10 :parse-fn parse-long]
   [nil "--wide-targets" "kill and pause also target a majority or all nodes"]
   [nil "--recovery SECS" "fault-free phase after the final heal; the recovery checker needs an ok op in it (lock quarantine is about 40 s at --max-lock-ttl 6)"
    :default 90 :parse-fn parse-long :validate [pos? "must be at least 1"]]
   [nil "--rate HZ" :default 50 :parse-fn parse-double]
   [nil "--keep-bin PATH" "local keep binary uploaded to every node" :default "/opt/keep/bin/keep"]
   [nil "--keep-jepsen PATH" "keep-jepsen binary on the control node (lock checker, readback owners)" :default "/opt/keep/bin/keep-jepsen"]
   [nil "--proxy-port PORT" "keep-jepsen proxy on the control node" :default 7100 :parse-fn parse-long]
   [nil "--tls-dir DIR" "rpk: run every node with --tls rpk using n<id>.key, servers.pem and clients.pem from keep-jepsen rpk-keys in DIR on the control node (run.sh KEEP_TLS=rpk); absent: plaintext"]
   [nil "--memory BYTES" "memory cap of every node; absent: the default cap" :parse-fn parse-long]
   [nil "--shards N" :default 2 :parse-fn parse-long]
   [nil "--max-lock-ttl SECS" :default 6 :parse-fn parse-long]
   [nil "--keep-time-sync" "leave chrony and systemd-timesyncd running in clock runs (slew test)"]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn keep-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
