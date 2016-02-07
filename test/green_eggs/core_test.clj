(ns green-eggs.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async
             :as async
             :refer [>! <! >!! <!! alts! go go-loop chan close! onto-chan put! timeout]]
            [green-eggs.core :refer :all]
            [clj-time.core :as t]))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))

(def green-eggs-n-ham
  ["in the rain"
   "on a train this one will skew the averages"
   "in a box"
   "with a fox"
   "in a house or x y z k"
   "with a mouse"
   "here or there"
   "anywhere"])

(defn char-count [words]
  (reduce (fn [result item]
            (let [c (count item)]
              (+ result c))) 0 words))

(defn distinct-letter-count [words]
  (count (set (apply str words))))

(defn interval-aggregator [aggregator in]
  "Execute the provided aggregator against the :items property from the maps on the channel"
  (let [out (chan)]
    (go-loop []
      (if-let [window (<! in)]
        (do
          (let [results (aggregator (:items window))]
            (>! out results))
          (recur))
        (close! out)))
    out))

(def distinct-letter-aggregations (partial interval-aggregator distinct-letter-count))
(def char-count-aggregations (partial interval-aggregator char-count))

(defn gen-timed-data [coll]
  (let [out (chan)]
    (go-loop [item (rand-nth coll)]
      (do
        (>! out [(t/now) item])
        (<! (timeout (rand-int 1000)))
        (recur (rand-nth coll))))
    out))

(defn stop-after-n-seconds [run-length]
  (let [out (chan)]
    (go
      (do
        (<! (timeout (* 1000 run-length)))
        (>! out :stop)))
    out))

(defn infinite-printer [data-ch]
  (let []
    (go-loop []
      (if-let [data (<! data-ch)]
        (do
          (clojure.pprint/pprint data)
          (recur))))))

(defn finite-printer [termination-ch data-ch]
  (let []
    (go-loop []
      (if-let [[data chan] (alts! [termination-ch data-ch])]
        (condp = chan
          termination-ch (println data)                     ; could check for val but meh!
          data-ch (do
                    (clojure.pprint/pprint data)
                    (recur)))))))

;(def window-chan (create-time-series-windows 3 3))
;(def data-chan (gen-timed-data green-eggs-n-ham))
;(def termination-chan (termination-protocol 10))
;(def time-series-chan (time-series-data data-chan window-chan))
;(def aggregated (distinct-letter-aggregations time-series-chan))
;(finite-printer termination-chan aggregated)

;(def window-chan (create-time-series-windows 4 2))
;(def data-chan (gen-timed-data green-eggs-n-ham))
;(def termination-chan (termination-protocol 30))
;(def time-series-chan (time-series-data data-chan window-chan))
;(def aggregated (char-count-aggregations time-series-chan))
;(finite-printer termination-chan aggregated)

;(def window-chan (create-time-series-windows 3 3))
;(def data-chan (gen-timed-data green-eggs-n-ham))
;(def termination-chan (termination-protocol 120))
;(def time-series-chan (time-series-data data-chan window-chan))
;(finite-printer termination-chan time-series-chan)

; TODO set up some scenarios that can be tested by take! from a final channel after the various
; pipelines have been established and comparing that with some calculated data against the data
; and time (for example 10 elements emitted in 10 seconds should be captured in X * N second windows
; and still equate to 10 elements - the aggregations can be computed by passing the data)

; Wikipedia sourced list of body and main parts
(def manifest ["Bonnet/hood" "Bonnet/hood latch" "Bumper" "Unexposed bumper" "Exposed bumper"
               "Cowl screen" "Decklid" "Fascia rear and support" "Fender (wing or mudguard)"
               "Front clip" "Front fascia and header panel" "Grille (also called grill)"
               "Pillar and hard trim" "Quarter panel" "Radiator core support"
               "Rocker" "Roof rack" "Spoiler" "Front spoiler (air dam)" "Rear spoiler (wing)" "Rims" "Hubcap"
               "Tire/Tyre" "Trim package" "Trunk/boot/hatch" "Trunk/boot latch" "Valance" "Welded assembly"
               "Outer door handle" "Inner door handle" "Door control module" "Door seal"
               "Door watershield" "Hinge" "Door latch" "Door lock and power door locks" "Center-locking"
               "Fuel tank (or fuel filler) door" "Window glass" "Sunroof" "Sunroof motor" "Window motor"
               "Window regulator" "Windshield (also called windscreen)" "Windshield washer motor"
               "Window seal"])

; magic up 460 items with variants for each part (adjust the range to create more / less)
(def parts (mapcat (fn [part]
                     (map #(assoc {} :id (gensym) :description (str part %)) (range 10)))
                   manifest))

(defn gen-random-deliveries [max-wait-millis coll]
  (let [out (chan)]
    (go-loop []
      (do
        (>! out [(t/now) (rand-nth coll)])
        (<! (timeout (rand-int max-wait-millis)))
        (recur)))
    out))

(defn gen-timed-orders [frequency-millis coll]
  (let [out (chan)]
    (go-loop []
      (do
        (>! out [(t/now) (rand-nth coll)])
        (<! (timeout frequency-millis))
        (recur)))
    out))

(defn modify-stock [count-adjust-fn stock item]
  (let [current-value (first (clojure.set/select #(= (:id %) (:id item)) stock))
        new-value (assoc current-value :count (count-adjust-fn (:count current-value)))
        updated-stock (conj stock new-value)]
    [updated-stock new-value]))

(defn stock-levels [orders deliveries]
  "Stock levels - also includes demand (negative stock numbers)"
  (let [out (chan)]
    (go-loop [stock (into #{} (map #(assoc {} :id (:id %) :count 0) parts))]
      (if-let [[data chan] (alts! [orders deliveries])]
        (let [[_ item] data
              update-operation (condp = chan
                                 orders (partial modify-stock dec)
                                 deliveries (partial modify-stock inc))]
          (if-let [[modified-stock updated-item] (update-operation stock item)]
            (do
              (>! out updated-item)
              (recur modified-stock))))
        (close! out)))
    out))

(defn detect-backlogs [low-water-mark stock-levels]
  "Show where stock levels fall below a low water mark "
  (let [out (chan 1 (filter (fn [stock-level-for-part]
                              (let [backlog (:count stock-level-for-part)]
                                (< backlog low-water-mark)))))]
    (go-loop []
      (if-let [stock-level-for-part (<! stock-levels)]
        (do
          (>! out stock-level-for-part)
          (recur))
        (close! out)))
    out))

(defn detect-order-peaks [high-water-mark in]
  "Show orders that occur more often a high water mark within any given window"
  (let [out (chan 1 (comp (mapcat (fn [window]
                                    (frequencies (:items window))))
                          (filter (fn [order]
                                    (let [[_ freq] order]
                                      (> freq high-water-mark))))))]
    (go-loop []
      (if-let [windows (<! in)]
        (do
          (>! out windows)
          (recur))
        (close! out)))
    out))


(def order-chan (async/mult (gen-timed-orders 20 parts)))  ; use mult to allow many readers

;(def deliveries-chan (gen-random-deliveries 1000 parts))

; show what's happening with stock
;(def stock-chan (stock-levels (async/tap order-chan (chan)) deliveries-chan))
;(def backlogs-chan (detect-backlogs -4 stock-chan))
;(finite-printer (stop-after-n-seconds 20) backlogs-chan)

; show what's happening with order peaks
(def windows-chan (create-time-series-windows 5))
(def time-series (time-series-data (async/tap order-chan (chan)) windows-chan))
(def peaks (detect-order-peaks 2 time-series))
(finite-printer (stop-after-n-seconds 30) peaks)







