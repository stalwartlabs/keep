;; SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
;;
;; SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL

(ns jepsen.keep.readback
  "Readback under restarts with staggered heals. The heal-restart nemesis cuts Y off, halts and restarts X, waits until X serves and every node but Y settles, holds, then heals Y's links except X-Y and, after a drawn skew, X-Y. Clients write fresh keys; after each round's heal settles, the nemesis reads back every key written since the round began. A write is judged when it was acknowledged inside the round's window (from X serving and the others settled, to the staggered heal) and its key's owner in the full placement is X; it must read back its value."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [store :as store]
                    [util :as util]]
            [jepsen.net.proto :as p]
            [jepsen.keep.client :as kc]
            [jepsen.keep.db :as kdb]
            [jepsen.keep.net :as kn]))

(def cut-lead-ms 2000)
(def hold-ms [1000 5000])
(def skew-ms [100 1000])
(def settle-margin-ms 1550)
(def poll-ms 200)
(def stop-wait-s 20)
(def verify-budget-ms 10000)
(def verify-ms-per-key 5)
(def retry-ms 250)
(def control-prefix "keep: control epoch")

(defn- between [[low high]]
  (+ low (rand-int (inc (- high low)))))

(defn- ready-timeout-ms [test]
  (+ 20000 (* 2000 (:max-lock-ttl test))))

(defn- last-control-line
  "The last control line of keep.log after its first `skip` lines on the current node, or nil."
  [skip]
  (let [line (str/trim (c/exec :sh :-c (str "tail -n +" (inc skip) " " kdb/logfile
                                            " | grep '^" control-prefix "' | tail -n 1")))]
    (when-not (str/blank? line) line)))

(defn- settled? [line]
  (str/ends-with? line "settled true"))

(defn- serving? [line]
  (and (str/includes? line "fenced false") (str/includes? line "awaiting 0")))

(defn- marks [test nodes]
  (c/on-nodes test nodes (fn [_ _] (kdb/log-lines))))

(defn- wait-for
  "Polls until every node's last control line after its mark satisfies pred in one sweep, or the deadline passes."
  [test marks pred]
  (let [deadline (+ (System/currentTimeMillis) (ready-timeout-ms test))]
    (loop []
      (let [lines (c/on-nodes test (keys marks) (fn [_ node] (last-control-line (get marks node))))]
        (cond
          (every? #(some-> % pred) (vals lines)) true
          (> (System/currentTimeMillis) deadline) false
          :else (do (Thread/sleep poll-ms) (recur)))))))

(defn- halt! [test node halt]
  (c/on-nodes test [node]
              (fn [test node]
                (case halt
                  :crash (db/kill! (:db test) test node)
                  :stop (kdb/terminate! stop-wait-s)))))

(defn- read-key
  "Reads one key until it answers or the deadline passes; returns [conn value], value nil when absent, :unverified when never answered."
  [test conn k deadline]
  (loop [conn conn]
    (if (> (System/currentTimeMillis) deadline)
      [conn :unverified]
      (let [reply (try (kc/call conn {:f "read" :key k})
                       (catch java.io.IOException _ nil))]
        (cond
          (= "ok" (:type reply)) [conn (:value reply)]
          (and reply (not= "proxy closed" (:error reply))) (do (Thread/sleep retry-ms) (recur conn))
          :else (do (kc/close! conn)
                    (Thread/sleep retry-ms)
                    (recur (kc/connect "127.0.0.1" (:proxy-port test)))))))))

(defn- read-back
  "Reads every key in [from, to) through its own proxy connection, within one shared budget."
  [test from to]
  (let [deadline (+ (System/currentTimeMillis) verify-budget-ms (* verify-ms-per-key (- to from)))]
    (loop [conn (kc/connect "127.0.0.1" (:proxy-port test))
           k from
           reads (transient [])]
      (if (< k to)
        (let [[conn value] (read-key test conn k deadline)]
          (recur conn (inc k) (conj! reads value)))
        (do (kc/close! conn)
            (persistent! reads))))))

(defn- draw [test]
  (let [[y x] (take 2 (shuffle (:nodes test)))]
    {:x x :y y
     :halt (rand-nth [:crash :stop])
     :hold-ms (between hold-ms)
     :skew-ms (between skew-ms)}))

(defn- heal! [test nets]
  (doseq [net nets] (p/heal! net test)))

(defn- round!
  [test [cut-net link-net :as nets] next-key {:keys [x y halt hold-ms skew-ms] :as round}]
  (let [others (remove #{y} (:nodes test))
        peers (remove #{x} others)
        from (inc @next-key)]
    (try
      (p/drop-all! cut-net test (assoc (zipmap peers (repeat [y])) y peers))
      (p/drop-all! link-net test {x [y] y [x]})
      (Thread/sleep cut-lead-ms)
      (halt! test x halt)
      (let [mark (marks test [x])
            _ (c/on-nodes test [x] (fn [test node] (db/start! (:db test) test node)))
            ready (wait-for test mark serving?)
            settled (wait-for test (marks test others) settled?)
            _ (when settled (Thread/sleep settle-margin-ms))
            ready-ns (util/relative-time-nanos)
            _ (Thread/sleep hold-ms)
            heal-ns (util/relative-time-nanos)
            to (inc @next-key)]
        (p/heal! cut-net test)
        (Thread/sleep skew-ms)
        (p/heal! link-net test)
        (let [healed (wait-for test (marks test (:nodes test)) settled?)]
          (info "heal-restart round x" x "y" y "ready" ready "settled" settled "healed" healed "keys" from "to" to)
          (assoc round
                 :x-id (kdb/node-id test x)
                 :members (kdb/members test)
                 :ready ready :settled settled :healed healed
                 :ready-ns ready-ns :heal-ns heal-ns
                 :from from :to to
                 :reads (read-back test from to))))
      (catch Throwable t
        (heal! test nets)
        (throw t)))))

(defn heal-restart-nemesis [next-key]
  (let [nets [(kn/chain-net "keep-heal") (kn/chain-net "keep-heal-link")]]
    (reify
      nemesis/Reflection
      (fs [_] #{:heal-restart})
      nemesis/Nemesis
      (setup! [this test] (heal! test nets) this)
      (invoke! [_ test op]
        (assoc op :value (round! test nets next-key (draw test))))
      (teardown! [_ test] (heal! test nets)))))

(defn- owners
  "Owner node id of every index below n in the full placement, from keep-jepsen proxy-owners (keep-proto's placement)."
  [test members n]
  (let [file (str (store/path test "owners.tsv"))
        {:keys [exit err]} (sh (:keep-jepsen test) "proxy-owners" "--members" members
                               "--keys" (str n) "--out" file)]
    (when-not (zero? exit)
      (throw (ex-info "keep-jepsen proxy-owners failed" {:exit exit :err err})))
    (with-open [r (io/reader file)]
      (persistent!
        (reduce (fn [m line]
                  (let [[_ key _ owner] (str/split line #"\t")]
                    (assoc! m (Long/parseUnsignedLong (subs key 2) 16) (parse-long owner))))
                (transient {})
                (rest (line-seq r)))))))

(defn acked-writes
  "Every acknowledged write as {:key :invoke-ns :ok-ns}."
  [history]
  (loop [ops (seq history) pending {} acked (transient [])]
    (if-let [[op & more] ops]
      (cond
        (not (and (integer? (:process op)) (= :write (:f op)))) (recur more pending acked)
        (= :invoke (:type op)) (recur more (assoc pending (:process op) op) acked)
        :else (let [invoke (get pending (:process op))
                    acked (if (and invoke (= :ok (:type op)))
                            (conj! acked {:key (:value op) :invoke-ns (:time invoke) :ok-ns (:time op)})
                            acked)]
                (recur more (dissoc pending (:process op)) acked)))
      (persistent! acked))))

(defn write-invocations
  "Invocation times of every client write."
  [history]
  (->> history
       (filter #(and (integer? (:process %)) (= :write (:f %)) (= :invoke (:type %))))
       (mapv :time)))

(defn- judgeable? [round]
  (and (:ready round) (:settled round)))

(defn- idle?
  "A round whose window opens at or after the end of the client phase and in which no client sent a write: it tests nothing."
  [client-end-ns invocations {:keys [ready-ns heal-ns]}]
  (and (<= client-end-ns ready-ns)
       (not-any? #(and (<= ready-ns %) (< % heal-ns)) invocations)))

(defn- outcome [rounds {:keys [key]}]
  (if-let [{:keys [from reads]} (some #(when (and (<= (:from %) key) (< key (:to %))) %) rounds)]
    (let [value (nth reads (- key from))]
      (cond
        (= value key) :read-back
        (nil? value) :absent
        (= value :unverified) :unverified
        :else :stale))
    :unread))

(defn- judging-round
  "Index of the judgeable round whose window holds the write and whose restarted member owns its key, or nil."
  [rounds owner-of {:keys [key invoke-ns ok-ns]}]
  (first (keep-indexed (fn [i round]
                         (when (and (judgeable? round)
                                    (<= (:ready-ns round) invoke-ns)
                                    (< ok-ns (:heal-ns round))
                                    (= (:x-id round) (owner-of key)))
                           i))
                       rounds)))

(defn judge
  "Judges rounds (heal-restart values), crashed rounds and acknowledged writes against owner-of (key to owner node id); invocations are the send times of every write, client-end-ns the end of the client phase on the history clock."
  [rounds crashed invocations writes owner-of client-end-ns]
  (let [judged (keep (fn [write] (when-let [i (judging-round rounds owner-of write)]
                                   (assoc write :round i :outcome (outcome rounds write))))
                     writes)
        judged-keys (set (map :key judged))
        per-round (frequencies (map :round judged))
        bad (remove #(= :read-back (:outcome %)) judged)
        judgeable (keep-indexed (fn [i round] (when (judgeable? round) i)) rounds)
        idle (set (filter #(idle? client-end-ns invocations (nth rounds %)) judgeable))
        blind (count (remove #(or (per-round %) (idle %)) judgeable))]
    {:valid? (and (pos? (count judged)) (zero? blind) (zero? crashed) (empty? bad))
     :rounds (count rounds)
     :crashed crashed
     :skipped (- (count rounds) (count judgeable))
     :idle (count idle)
     :blind blind
     :judged (count judged)
     :violations (count bad)
     :lost (->> bad
                (take 10)
                (map #(assoc % :x (:x (nth rounds (:round %))) :y (:y (nth rounds (:round %))))))
     :per-round (map-indexed (fn [i round]
                               (assoc (select-keys round [:x :y :halt :hold-ms :skew-ms :ready :settled :healed :from :to])
                                      :judged (get per-round i 0)
                                      :idle (contains? idle i)))
                             rounds)
     :unjudged (->> writes
                    (remove #(judged-keys (:key %)))
                    (map #(outcome rounds %))
                    frequencies)}))

(defn checker []
  (reify checker/Checker
    (check [_ test history opts]
      (let [heal-ops (filter #(and (= :heal-restart (:f %)) (= :info (:type %))) history)
            rounds (->> heal-ops (map :value) (filter #(and (map? %) (contains? % :reads))) vec)
            crashed (count (filter #(or (:exception %) (:error %)) heal-ops))
            writes (acked-writes history)
            members (:members (first rounds))
            owner-of (if (and members (seq writes))
                       (owners test members (inc (reduce max (map :key writes))))
                       {})]
        (judge rounds crashed (write-invocations history) writes owner-of
               (* (:time-limit test) 1000000000))))))

(defrecord WriteClient [conn next-key]
  client/Client
  (open! [this test node]
    (assoc this :conn (kc/connect "127.0.0.1" (:proxy-port test))))
  (setup! [_ test])
  (invoke! [_ test op]
    (let [k (swap! next-key inc)
          op (assoc op :value k)]
      (try
        (kc/apply-reply op (kc/call conn {:f "write" :key k :value k}) (constantly k))
        (catch java.net.SocketTimeoutException _
          (assoc op :type :info :error :timeout)))))
  (teardown! [_ test])
  (close! [_ test]
    (when conn (kc/close! conn))))

(defn workload
  "Blind writes of fresh keys (value = key). The client draws each key at invoke time from next-key, which the heal-restart nemesis shares to bound each round's read-back."
  [next-key]
  {:next-key next-key
   :client (map->WriteClient {:next-key next-key})
   :generator (repeat {:f :write})
   :checker (checker)})

(defn package [opts next-key]
  {:nemesis (heal-restart-nemesis next-key)
   :generator (gen/cycle [(gen/sleep (:nemesis-interval opts))
                          {:type :info :f :heal-restart}])
   :final-generator nil
   :perf #{}})
