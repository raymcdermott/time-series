(ns green-eggs.core
  (:require [clojure.core.async
             :as a
             :refer [>! <! >!! <!! alts! go go-loop chan buffer close! onto-chan put! timeout]]
            [clj-time.core :as t]
            [clj-time.coerce :refer [from-date]]))

; TODO experiment with running his via nashron (JDK8 JS VM) to see how it compares to node

; TODO spilt out the testing code

;If you were to implement yourself, you could make a `go` that simply pulls from a channel, and adds to another as a
;`[timestamp item]` pair, then finally pushes into an unbounded `chan` that has a transducer that filters based on
; age of that timestamp

(def green-eggs-n-ham
  ["in the rain"
   "on a train"
   "in a box"
   "with a fox"
   "in a house"
   "with a mouse"
   "here or there"
   "anywhere"])

; TODO change logic to remove the need for this
(def retention-period (t/seconds 10))

(defn maintain-active-windows [windows]
  (let [now (t/now)
        retention-boundary (t/minus now retention-period)
        retained (filter #(t/after? (:to %) retention-boundary) windows)
        to-be-closed (filter #(and (t/before? (:to %) now) (false? (:closed %))) windows)
        closing (map #(assoc % :closed true) to-be-closed)]
    (concat retained closing)))

(defn within-interval? [from to time]
  {:pre [(t/before? from to)]}
  "Check whether a time is within an interval"
  (let [interval (t/interval from to)]
    (t/within? interval time)))

(defn add-timed-item-to-windows [timed-item windows]
  "Add an item to the windows where the time intervals match"
  (if-let [[time item] timed-item]
    (let [matching-windows (filter #(within-interval? (:from %) (:to %) time) windows)
          updated-windows (map #(assoc % :items (conj (:items %) item)) matching-windows)]
      updated-windows)))

(defn gen-timed-data [coll]
  (let [out (chan)]
    (go-loop [item (rand-nth coll)]
      (do
        (>! out [(t/now) item])
        (<! (timeout (rand-int 3000)))
        (recur (rand-nth coll))))
    out))

(defn termination-protocol [run-length]
  (let [out (chan)]
    (go
      (do
        (<! (timeout (* 1000 run-length)))
        (>! out :stop)))
    out))

(defn gen-window [start-time open-duration]
  {:pre [(> open-duration 0)]}
  (let [to-time (t/plus start-time (t/seconds open-duration))]
    (assoc {} :from start-time :to to-time :closed false :items [])))

(defn create-time-series-windows [open-duration slide-interval]
  "Create a window for n seconds that slides every n seconds"
  {:pre [(> open-duration 0) (> slide-interval 0)]}
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

(defn time-series-data
  "Allocate items to time windows"
  [item-ch window-ch]
  (let [out (chan)]
    (go-loop [active-windows ()]
      (if-let [[data chan] (alts! [item-ch window-ch])]
        (condp = chan
          window-ch (if-let [windows (maintain-active-windows (conj active-windows data))]
                      (do                                   ; put this filter on the consumer!!
                        (if-let [closed (filter #(:closed %) windows)]
                          (>! out closed))
                        (recur windows)))

          item-ch (if-let [windows (add-timed-item-to-windows data active-windows)]
                    (recur windows)))
        (close! out)))
    out))


; TODO: persist the last processed id on each window boundary

; TODO: make a filter for closed windows only, allow function to be passed
(defn interval-aggregator [in]
  (let [out (chan)]
    (go-loop []
      (if-let [windows (<! in)]
        (do
          (if-let [totals (remove nil? (map (fn [window]
                                              (let [map-data (last window)
                                                    items (:items map-data)]
                                                (if (:closed map-data)
                                                  (if-let [total (reduce (fn [result item]
                                                                           (let [c (count item)]
                                                                             (+ result c))) 0 items)]
                                                    [total (:id map-data)]))))
                                            windows))]
            (if (not (empty? totals))
              (>! out totals)))
          (recur))
        (close! out)))
    out))

(defn infinite-printer [data-ch]
  (go-loop []
    (if-let [data (<! data-ch)]
      (do
        (clojure.pprint/pprint data)
        (recur)))))

(defn finite-printer [termination-ch data-ch]
  (go-loop []
    (if-let [[data chan] (alts! [termination-ch data-ch])]
      (condp = chan
        termination-ch (println data)
        data-ch (do
                  (clojure.pprint/pprint data)
                  (recur))))))

;(def in-chan (chan))
;(def timestamped-data (time-stamper add-timestamp-with-delay in-chan))
;(def windows-channel (create-time-series-windows 2 2))
;(def windowed (time-series-data timestamped-data windows-channel))
;(def aggregated (interval-aggregator windowed))


;(def window-chan (create-time-series-windows 3 3))
;(def data-chan (gen-timed-data green-eggs-n-ham))
;(def termination-chan (termination-protocol 120))
;(def time-series-chan (time-series-data data-chan window-chan))
;(finite-printer termination-chan time-series-chan)