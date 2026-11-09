;; SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
;;
;; SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL

(defproject jepsen.keep "0.1.0"
  :description "Jepsen test for Stalwart Keep"
  :license {:name "AGPL-3.0-only OR LicenseRef-SEL"}
  :main jepsen.keep
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [cheshire "5.12.0"]]
  :jvm-opts ["-Xmx8g" "-server"])
