(ns green-eggs.core
  (:require [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go go-loop chan buffer close! onto-chan]]
            [clj-time.core :as t]
            [clj-time.coerce :refer [from-date]]))

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

(defn delaying-timestamper [millis]
  (let [now (t/now)]
    (Thread/sleep millis)
    now))

(defn add-timestamp [now-provider item]
  (let [now (now-provider)]
    [now item]))

(defn time-stamper
  [timestamper in]
  (let [out (chan)]
    (go-loop []
      (if-let [item (<! in)]
        (do
          (>! out (timestamper item))
          (recur))
        (close! out)))
    out))

; for testing the window
(def add-timestamp-with-delay (partial add-timestamp (partial delaying-timestamper 500)))
(def add-timestamp-no-delay (partial add-timestamp t/now))

; TODO change this to a set based on the window ID
(def time-windows (atom []))

(defn add-window [window]
  (swap! time-windows conj window))

(defn update-window [window item]
  ;TODO add item
  )

; TODO eventually support env var that varies the retention period (0 -> n)
(def retention-period (t/seconds 30))

(defn archive-windows! []
  "maintains the definitions of current windows"
  (let [now (t/now)
        active-windows (filter #(t/after? now (:to %)) @time-windows)
        inactive-windows (filter #(t/before? now (:to %)) @time-windows)
        closed-windows (map #(assoc % :closed true) inactive-windows)
        retention-boundary (t/minus now retention-period)
        updated-windows (filter #(t/before? retention-boundary (:to %)) closed-windows)
        corrected-windows (concat active-windows updated-windows)]
    (clojure.pprint/pprint "UPDATED WINDOWS:" corrected-windows)
    (reset! time-windows corrected-windows)))

(defn within-interval? [from to time]
  "Check whether a time is within an interval"
  (let [interval (t/interval from to)]
    (t/within? interval time)))

(defn add-item-to-active-windows! [item]
  (let [[item-time _] item
        matching-windows (filter #(within-interval? (:from %) (:to %) item-time) @time-windows)]
    (map #(assoc % :items (conj (:items %) item)) matching-windows)))

(defn window [open-duration slide-interval]
  "Create a window for n seconds that slides every n seconds"
  {:pre [(> open-duration 0)
         (> slide-interval 0)]}
  (let [out (chan)]
    (go-loop [start-time false]
      (let [id (gensym)
            t0 (or start-time (t/now))
            t1 (t/plus t0 (t/seconds open-duration))
            w (assoc {} :id id :from t0 :to t1 :items [])
            new-window ["Window" w]]
        (do
          (add-window new-window)
          (>! out new-window)
          (Thread/sleep (* 1000 slide-interval))
          (recur (and (= open-duration slide-interval) t1)))))
    out))

(defn window-filter
  "Allocate items to appropriate time windows"
  [in]
  (let [out (chan)]
    (go-loop []
      (if-let [item (<! in)]
        (let [[part1 _] item]
          (cond
            (= part1 "Window") (archive-windows!)
            (= org.joda.time.DateTime (type part1)) (if-let [output (add-item-to-active-windows! item)]
                                                      (>! out output)))
          (recur))
        (close! out)))
    out))

; TODO: this on window boundary
(defn interval-aggregator [in]
  (let [out (chan)
        wc (atom 0)]
    (go-loop []
      (if-let [item (<! in)]
        (do
          (>! out (swap! wc + (count item)))
          (recur))
        (close! out)))
    out))

(defn simple-printer [in]
  (go-loop []
    (if-let [item (<! in)]
      (do
        (clojure.pprint/pprint item)
        (recur)))))

(def in-chan (chan))

(def standard-channel (time-stamper add-timestamp-with-delay in-chan))

;(def windows-channel (window 10 10))
;
;(def merged-channel (a/merge [standard-channel windows-channel]))
;
;(def windowed (window-filter merged-channel))
;
;(def aggregated (interval-aggregator windowed))
;
;(simple-printer aggregated)

;(onto-chan in-chan green-eggs-n-ham)
