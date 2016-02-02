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

; TODO: design for sliding and overlapping windows (tap into a time channel?)
; generate windows on a timing channel
; keep a list of active windows (need an atom?)
; emit active windows for each item? [yes, let's start with this]
; or emit event N times? [no]

(def active-windows (atom []))

(defn update-windows! [window]
  (let [all-windows (swap! active-windows conj window)
        closed? (partial (t/before? (t/now)))
        active (filter #(closed? (:to %)) all-windows)]
    (reset! active-windows active)))

(defn within-window? [window-open window-close item-time]
  (let [interval (t/interval window-open window-close)]
    (t/within? interval item-time)))

(defn in-which-windows? [item]
  (let [[item-time _] item
        matching-windows (filter #(within-window? (:from %) (:to %) item-time) @active-windows)]
    (map #([item %]) matching-windows)))


(defn window [open-duration slide-interval]
  "Create a window for n seconds that slides every n seconds"
  {:pre [(> open-duration 0)
         (> slide-interval 0)]}
  (let [out (chan)]
    (go-loop [start-time false]
      (let [id (gensym)
            t0 (or start-time (t/now))                      ; no gaps!
            t1 (t/plus t0 (t/seconds open-duration))
            w (assoc {} :id id :from t0 :to t1)
            new-window ["Window" w]]
        (do
          (>! out new-window)
          (Thread/sleep (* 1000 slide-interval))
          (recur (and (= open-duration slide-interval) t1)))))
    out))

(defn window-filter
  [in]
  (let [out (chan)]
    (go-loop []
      (if-let [item (<! in)]
        (let [[part1 part2] item]
          (cond
            (= part1 "Window") (update-windows! part2)
            (= org.joda.time.DateTime (type part1)) (if-let [output (in-which-windows? part2)]
                                                      (>! out output))
            :else (println "unknown item - dropping" item))
          (recur))
        (close! out)))
    out))

; should do this on window boundary
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

(def windows-channel (window 10 10))

(def merged-channel (a/merge standard-channel windows-channel))

(def windowed (window-filter merged-channel))

(def aggregated (interval-aggregator windowed))

(simple-printer aggregated)

;(onto-chan in-chan green-eggs-n-ham)
