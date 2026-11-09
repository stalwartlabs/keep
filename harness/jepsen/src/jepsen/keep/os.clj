;; SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
;;
;; SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL

(ns jepsen.keep.os
  "Debian 13 (trixie) node setup: jepsen.os.debian with package names that exist on trixie."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [os :as os]
                    [util :refer [meh]]]
            [jepsen.os.debian :as debian]
            [jepsen.keep.net :as kn]))

(def packages
  [:apt-transport-https :wget :curl :netcat-openbsd :ntpsec-ntpdate :faketime :unzip :iptables :psmisc
   :procps :tar :bzip2 :iputils-ping :iproute2 :rsyslog :logrotate :dirmngr :tcpdump :build-essential])

(def time-sync-services ["chrony" "systemd-timesyncd"])

(def reset-rate "/usr/local/bin/keep-reset-rate")

(def reset-rate-source
  "#include <sys/timex.h>
int main(void) {
  struct timex t = {0};
  t.modes = ADJ_TICK | ADJ_FREQUENCY;
  t.tick = 10000;
  t.freq = 0;
  return adjtimex(&t) < 0;
}
")

(defn stop-time-sync?
  "Clock runs stop NTP because a slewing daemon changes the rate of CLOCK_MONOTONIC and CLOCK_BOOTTIME."
  [test]
  (and (some #{:clock} (:faults test)) (not (:keep-time-sync test))))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up debian trixie")
      (debian/setup-hostfile!)
      (debian/maybe-update!)
      (c/su (debian/install packages))
      (meh (kn/clear!))
      (when (stop-time-sync? test)
        (info node "stopping chrony and systemd-timesyncd for the clock nemesis")
        (c/su (doseq [service time-sync-services]
                (meh (c/exec :systemctl :stop service)))
              (c/exec :printf "%s" reset-rate-source :> "/tmp/keep-reset-rate.c")
              (c/exec :gcc :-o reset-rate "/tmp/keep-reset-rate.c")
              (c/exec reset-rate))))
    (teardown! [_ test node]
      (when (stop-time-sync? test)
        (c/su (doseq [service time-sync-services]
                (meh (c/exec :systemctl :start service))))))))
