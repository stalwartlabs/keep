;; SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
;;
;; SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL

(ns jepsen.keep.client
  "A Jepsen client that speaks JSON lines to keep-jepsen proxy on the control node; the proxy holds one keep-client per connection."
  (:require [cheshire.core :as json]
            [jepsen.client :as client])
  (:import (java.io BufferedReader InputStreamReader PrintWriter)
           (java.net Socket)))

(defn connect [host port]
  (let [socket (Socket. ^String host (int port))]
    (.setTcpNoDelay socket true)
    (.setSoTimeout socket 10000)
    {:socket socket
     :in (BufferedReader. (InputStreamReader. (.getInputStream socket) "UTF-8"))
     :out (PrintWriter. (.getOutputStream socket) true)}))

(defn call [conn request]
  (.println ^PrintWriter (:out conn) (json/generate-string request))
  (let [line (.readLine ^BufferedReader (:in conn))]
    (if line
      (json/parse-string line true)
      {:type "info" :error "proxy closed"})))

(defn close! [conn]
  (.close ^Socket (:socket conn)))

(defn apply-reply [op reply value-fn]
  (case (:type reply)
    "ok" (assoc op :type :ok :value (value-fn (:value reply)))
    "fail" (assoc op :type :fail :error (:error reply))
    (assoc op :type :info :error (:error reply))))

(defrecord ProxyClient [conn request-fn value-fn]
  client/Client
  (open! [this test node]
    (assoc this :conn (connect "127.0.0.1" (:proxy-port test))))
  (setup! [_ test])
  (invoke! [_ test op]
    (try
      (apply-reply op (call conn (request-fn op)) #(value-fn op %))
      (catch java.net.SocketTimeoutException _
        (assoc op :type :info :error :timeout))))
  (teardown! [_ test])
  (close! [_ test]
    (when conn (close! conn))))

(defn proxy-client [request-fn value-fn]
  (map->ProxyClient {:request-fn request-fn :value-fn value-fn}))
