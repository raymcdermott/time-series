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

(def time-windows (atom {}))

; TODO support env var that varies the retention period (0 -> n)
(def retention-period (t/seconds 30))

(defn assoc-window [window]
  "Maintain the map of current window(s)"
  (let [all-windows (swap! time-windows assoc (:id window) window)
        window-maps (map last all-windows)
        now (t/now)
        to-be-closed (filter #(and (t/before? (:to %) now) (= false (:closed %))) window-maps)
        retention-boundary (t/minus now retention-period)
        to-be-dropped (filter #(and (= true (:closed %)) (t/before? (:to %) retention-boundary)) window-maps)]
    (doall (map #(swap! time-windows assoc (:id %) (assoc % :closed true)) to-be-closed))
    (doall (map #(swap! time-windows dissoc (:id %)) to-be-dropped))))

(defn add-window [window]
  (assoc-window window))

(defn update-window [window]
  (assoc-window window))

(defn within-interval? [from to time]
  "Check whether a time is within an interval"
  (let [interval (t/interval from to)]
    (t/within? interval time)))

(defn put-item-in-window [item]
  (let [[item-time _] item
        matching-windows (filter #(within-interval? (:from %) (:to %) item-time) @time-windows)
        updated-windows (map #(assoc % :items (conj (:items %) item)) matching-windows)]
    (doall (map #(update-window %) updated-windows))))

(defn create-window [open-duration slide-interval]
  "Create a window for n seconds that slides every n seconds"
  {:pre [(> open-duration 0)
         (> slide-interval 0)]}
  (let [out (chan)]
    (go-loop [start-time false]
      (let [id (gensym)
            t0 (or start-time (t/now))
            t1 (t/plus t0 (t/seconds open-duration))
            w (assoc {} :id id :from t0 :to t1 :closed false :items [])
            window-tuple ["Window" w]]
        (do
          (>! out window-tuple)
          (Thread/sleep (* 1000 slide-interval))
          (recur (and (= open-duration slide-interval) t1)))))
    out))

(defn window-matching
  "Allocate items to time windows"
  [in]
  (let [out (chan)]
    (go-loop []
      (if-let [tuple (<! in)]
        (do
          (let [[elem1 elem2] tuple]
            (cond
              (= "Window" elem1) ((add-window elem2)
                                   (>! out tuple))

              (= org.joda.time.DateTime (type elem1)) (if-let [output (put-item-in-window tuple)] ; drop non-matching items
                                                        (doall (map #(>! out %) output))))
            (recur)))
        (close! out)))
    out))

; TODO: persist the last processed id on each window boundary

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
        (println item)
        (recur)))))

(def in-chan (chan))

;(def standard-channel (time-stamper add-timestamp-with-delay in-chan))
;
(def windows-channel (create-window 10 10))
;;
;(def merged-channel (a/merge [standard-channel windows-channel]))
;;
;(def windowed (window-matching merged-channel))
;;
;;;;(def aggregated (interval-aggregator windowed))
;;
(simple-printer windows-channel)
;
(onto-chan in-chan green-eggs-n-ham)
