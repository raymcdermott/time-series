(ns green-eggs.core
  (:require [clojure.core.async
             :as async
             :refer [>! <! >!! <!! alts! go go-loop chan buffer close! onto-chan put! timeout]]
            [clj-time.core :as t]
            [clj-time.coerce :refer [from-date]]))

;If you were to implement yourself, you could make a `go` that simply pulls from a channel, and adds to another as a
;`[timestamp item]` pair, then finally pushes into an unbounded `chan` that has a transducer that filters based on
; age of that timestamp

; TODO change logic to remove the need for this
(def retention-period (t/millis 750))

(defn- maintain-active-windows [windows]
  (let [now (t/now)
        retention-boundary (t/minus now retention-period)
        retained (filter #(t/after? (:to %) retention-boundary) windows)
        to-be-closed (filter #(and (t/before? (:to %) now) (false? (:closed %))) windows)
        closing (map #(assoc % :closed true) to-be-closed)]
    (concat retained closing)))

(defn- within-interval? [from to time]
  {:pre [(t/before? from to)]}
  "Check whether a time is within an interval"
  (let [interval (t/interval from to)]
    (t/within? interval time)))

(defn- add-timed-item-to-windows [timed-item windows]
  "Add an item to the windows where the time intervals match"
  (if-let [[time item] timed-item]
    (let [matching-windows (filter #(within-interval? (:from %) (:to %) time) windows)
          updated-windows (map #(assoc % :items (conj (:items %) item)) matching-windows)]
      updated-windows)))

(defn- gen-window [start-time open-duration]
  {:pre [(> open-duration 0)]}
  (let [to-time (t/plus start-time (t/seconds open-duration))]
    (assoc {} :from start-time :to to-time :closed false :items [])))

(defn create-time-series-windows
  "Create a window for X seconds that slides every Y seconds. 1 <= Y <= X"
  ([open-duration slide-interval]
   {:pre [(> open-duration 0) (> slide-interval 0) (>= open-duration slide-interval)]}
   (let [out (chan)]
     (go-loop [start-time (t/now)]
       (let [window (gen-window start-time open-duration)]
         (do
           (>! out window)
           (<! (timeout (* 1000 slide-interval)))
           (if (= open-duration slide-interval)
             (recur (:to window))
             (recur (t/now))))))
     out))

  ([open-duration]
   {:pre [(> open-duration 0)]}
   (create-time-series-windows open-duration open-duration)))

(defn time-series-data
  "Add timed data from item-ch to the time series windows produced in the window-ch"
  [item-ch window-ch]
  (let [out-ch (chan)]
    (go-loop [active-windows ()]
      (if-let [[data chan] (alts! [item-ch window-ch])]
        (condp = chan
          window-ch (if-let [windows (maintain-active-windows (conj active-windows data))]
                      (do
                        (let [closed (filter #(:closed %) windows)
                              count (count closed)]
                          (cond
                            (= count 1) (>! out-ch (first closed))
                            (> count 1) (println "Should never happen. Add to backlog")))
                        (recur windows)))

          item-ch (if-let [windows (add-timed-item-to-windows data active-windows)]
                    (recur windows)))
        (close! out-ch)))
    out-ch))