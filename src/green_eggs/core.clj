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

(defn in-current-window? [window-seconds window-open elem]
  (let [[time _] elem
        window-close (t/plus window-open (t/seconds window-seconds))
        window (t/interval window-open window-close)]
    (t/within? window time)))

; TODO: design for sliding and overlapping windows (tap into a time channel?)
; generate windows on a timing channel
; keep a list of active windows (need an atom?)
; emit active windows for each item? [yes, let's start with this]
; or emit event N times? [no]

(defn window-filter
  [n-secs in]
  (let [out (chan)
        in-window? (partial in-current-window? n-secs (t/now))]
    (go-loop []
      (if-let [item (<! in)]
        (let [[_ text] item]
          (do
            (if (in-window? item)
              (>! out text))
            (recur)))
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

(def windowed (window-filter 2 standard-channel))

(def aggregated (interval-aggregator windowed))

(simple-printer aggregated)

(onto-chan in-chan green-eggs-n-ham)
