;; SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
;;
;; SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL

(ns jepsen.keep.net
  "Per-nemesis iptables chains: a chain net drops into its own chain, jumped from INPUT, and heals by flushing only that chain, so one nemesis's heal never lifts another's cut. Packet shaping stays with jepsen's iptables net."
  (:require [clojure.string :as str]
            [jepsen [control :as c]
                    [net :as net]]
            [jepsen.control.net :as cn]
            [jepsen.net.proto :as p]))

(defn- ensure-chain! [chain]
  (c/su (c/exec :sh :-c (str "iptables -w -N " chain " 2>/dev/null || true; "
                             "iptables -w -C INPUT -j " chain " 2>/dev/null || iptables -w -A INPUT -j " chain))))

(defn- drop-from! [chain srcs]
  (ensure-chain! chain)
  (c/su (c/exec :iptables :-w :-A chain :-s (str/join "," (map cn/ip srcs)) :-j :DROP)))

(defn- flush-chain! [chain]
  (c/su (c/exec :sh :-c (str "iptables -w -F " chain " 2>/dev/null || true"))))

(defn clear!
  "Removes every rule and every user chain on the current node: a clean start, whatever an earlier run left."
  []
  (c/su
    (c/exec :iptables :-w :-F)
    (c/exec :iptables :-w :-X)))

(defn chain-net [chain]
  (reify
    p/Net
    (drop! [_ test src dest]
      (c/on-nodes test [dest] (fn [_ _] (drop-from! chain [src]))))
    (heal! [_ test]
      (c/on-nodes test (fn [_ _] (flush-chain! chain))))
    (slow! [_ test] (p/slow! net/iptables test))
    (slow! [_ test opts] (p/slow! net/iptables test opts))
    (flaky! [_ test] (p/flaky! net/iptables test))
    (fast! [_ test] (p/fast! net/iptables test))
    (shape! [_ test nodes behavior] (p/shape! net/iptables test nodes behavior))

    p/PartitionAll
    (drop-all! [_ test grudge]
      (c/on-nodes test (keys grudge)
                  (fn [_ node]
                    (when-let [srcs (seq (get grudge node))]
                      (drop-from! chain srcs)))))))
