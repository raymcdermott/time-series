(ns green-eggs.core
  (:require [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout onto-chan]]
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
    (a/go-loop []
      (if-let [item (<! in)]
        (if-let [enriched (timestamper item)]
          (>! out enriched)))
      (recur))
    out))

; for testing the window
(def add-timestamp-with-delay (partial add-timestamp (partial delaying-timestamper 200)))
(def add-timestamp-no-delay (partial add-timestamp t/now))

(defn in-current-window? [window-seconds window-open elem]
  (let [[time item] elem
        window-close (t/plus window-open (t/seconds window-seconds))
        window (t/interval window-open window-close)]
    (t/within? window time)))

; TODO: add an id for each interval so that downstream consumers can see when the window closes
(defn window-filter
  [in n-secs]
  (let [out (chan)
        window-id (atom 0)
        window-opening-time (atom (t/now))]
    (a/go-loop [id window-id
                opening-time window-opening-time]
      (if-let [item (<! in)]
        (let [in-window? (partial in-current-window? n-secs window-opening-time)
              is-in-window? (in-window? item)]
          (if (is-in-window?)
            (>! out [id item])
            (let [id (swap! window-id inc)
                  opening-time (swap! window-opening-time t/now)]
              (>! out [id item])))))
      (recur window-id opening-time))
    out))

(defn interval-aggregator [in]
  (let [out (chan)
        wc (atom 0)]
    (a/go-loop []
      (if-let [[id item] (<! in)]
        (>! out [id (swap! wc + (count item))]))
      (recur))
    out))

(defn simple-printer [in]
  (a/go-loop []
    (if-let [item (<! in)]
      (clojure.pprint/pprint item))
    (recur)))

(def in-chan (chan))

(def standard-channel (time-stamper add-timestamp-with-delay in-chan))

(def windowed (window-filter 2 standard-channel))

(def aggregated (interval-aggregator windowed))

(simple-printer aggregated)

;(onto-chan in-chan green-eggs-n-ham)
