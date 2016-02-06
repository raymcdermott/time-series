(ns green-eggs.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async
             :as async
             :refer [>! <! >!! <!! alts! go go-loop chan buffer close! onto-chan put! timeout]]
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
             (if-let [windows (<! in)]
               (do
                 (let [results (map #(aggregator (:items %)) windows)]
                   (if (= 1 (count results))
                     (>! out (first results))
                     (>! out results))
                   (recur)))
               (close! out)))
    out))

(def distinct-letter-aggregations (partial interval-aggregator distinct-letter-count))
(def char-count-aggregations (partial interval-aggregator char-count))

(defn infinite-printer [data-ch]
  (let [test-ch (chan)]
    (go-loop []
      (if-let [data (<! data-ch)]
        (do
          (clojure.pprint/pprint data)
          (>! test-ch data)
          (recur))))))

(defn finite-printer [termination-ch data-ch]
  (let [test-ch (chan)]
    (go-loop []
      (if-let [[data chan] (alts! [termination-ch data-ch])]
        (condp = chan
          termination-ch (println data)
          data-ch (do
                    (clojure.pprint/pprint data)
                    (>! test-ch data)
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