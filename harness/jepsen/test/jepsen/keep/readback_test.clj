;; SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
;;
;; SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL

(ns jepsen.keep.readback-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.keep.readback :as rb]))

(def round
  {:x "n2" :y "n1" :x-id 2 :ready true :settled true :healed true
   :ready-ns 100 :heal-ns 200 :from 10 :to 14 :reads [10 nil 12 :unverified]})

(def owner-of {10 2, 11 2, 12 3, 13 2, 14 2})

(def invoked [90 110])

(def client-end 1000)

(def later (assoc round :ready-ns 300 :heal-ns 400))

(defn- write [k invoke-ns ok-ns]
  {:key k :invoke-ns invoke-ns :ok-ns ok-ns})

(deftest a-judged-write-must-read-back
  ;; key 10 is owned by the restarted member, written in the window, and reads back
  (is (:valid? (rb/judge [round] 0 invoked [(write 10 110 120)] owner-of client-end)))
  ;; key 11 is judged the same way but reads absent
  (let [result (rb/judge [round] 0 invoked [(write 10 110 120) (write 11 110 120)] owner-of client-end)]
    (is (not (:valid? result)))
    (is (= 1 (:violations result)))
    (is (= :absent (:outcome (first (:lost result))))))
  ;; key 13 never answered the read-back
  (is (= :unverified (:outcome (first (:lost (rb/judge [round] 0 invoked [(write 13 110 120)] owner-of client-end)))))))

(deftest writes-outside-the-window-or-owner-are-reported-not-judged
  ;; sent before ready, acknowledged after the heal, or owned by another member
  (let [result (rb/judge [round] 0 invoked
                         [(write 10 110 120) (write 11 90 120) (write 13 110 200) (write 12 110 120)]
                         owner-of client-end)]
    (is (:valid? result))
    (is (= 1 (:judged result)))
    (is (= {:absent 1 :unverified 1 :read-back 1} (:unjudged result)))))

(deftest a-judged-write-outside-the-read-range-fails
  ;; key 14 was never read back, so it is unread
  (let [result (rb/judge [round] 0 invoked [(write 10 110 120) (write 14 110 120)] owner-of client-end)]
    (is (not (:valid? result)))
    (is (= :unread (:outcome (first (:lost result)))))))

(deftest blind-skipped-and-crashed-rounds
  (testing "a settled round without a judged write is blind"
    (let [result (rb/judge [round later] 0 (conj invoked 350) [(write 10 110 120)] owner-of client-end)]
      (is (= 1 (:blind result)))
      (is (not (:valid? result)))))
  (testing "an unsettled or not ready round is skipped, not blind, but a run with no judged write fails"
    (doseq [skipped [(assoc round :settled false) (assoc round :ready false)]]
      (let [result (rb/judge [skipped] 0 invoked [(write 10 110 120)] owner-of client-end)]
        (is (= [1 0 0] [(:skipped result) (:blind result) (:judged result)]))
        (is (not (:valid? result))))))
  (testing "a crashed round fails the run"
    (is (not (:valid? (rb/judge [round] 1 invoked [(write 10 110 120)] owner-of client-end))))))

(deftest idle-rounds-open-after-the-client-phase
  (testing "a round opening after the client phase with no write sent is idle and the run valid"
    (let [result (rb/judge [round later] 0 invoked [(write 10 110 120)] owner-of 250)]
      (is (= [1 0] [(:idle result) (:blind result)]))
      (is (:valid? result))))
  (testing "a round inside the client phase in which every client hung is blind and the run invalid"
    (let [result (rb/judge [round later] 0 invoked [(write 10 110 120)] owner-of client-end)]
      (is (= [0 1] [(:idle result) (:blind result)]))
      (is (not (:valid? result)))))
  (testing "an absent judged write fails the run with or without an idle round"
    (doseq [end [250 client-end]]
      (let [result (rb/judge [round later] 0 invoked [(write 10 110 120) (write 11 110 120)] owner-of end)]
        (is (= 1 (:violations result)))
        (is (not (:valid? result)))))))

(deftest write-invocations-are-client-write-sends
  ;; nemesis ops, reads and completions are not write sends
  (is (= [5 6] (rb/write-invocations [{:process 0 :type :invoke :f :write :time 5}
                                      {:process 1 :type :invoke :f :write :time 6}
                                      {:process :nemesis :type :info :f :heal-restart :time 7}
                                      {:process 0 :type :ok :f :write :time 9}]))))

(deftest acked-writes-pairs-invocations-by-process
  ;; only ok completions of client writes count; nemesis ops and failures are ignored
  (is (= [(write 10 5 9)]
         (rb/acked-writes [{:process 0 :type :invoke :f :write :value 10 :time 5}
                           {:process 1 :type :invoke :f :write :value 11 :time 6}
                           {:process :nemesis :type :info :f :heal-restart :time 7}
                           {:process 1 :type :fail :f :write :value 11 :time 8}
                           {:process 0 :type :ok :f :write :value 10 :time 9}]))))
