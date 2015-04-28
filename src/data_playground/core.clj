(ns data-playground.core
  (:require [cheshire.core :refer :all]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            ))

(def fight-keys [:Name :Weight :Country :Height :Birthday :Class])

(defn get-records [path]
  (json/read-str (slurp path) :key-fn keyword))

(defn create-index [x]
  (sort (map #(.indexOf fight-keys %) x)))

(defn validate-record [m]
  (let [test (clojure.set/difference (set fight-keys) (set (keys m)))]
    (if (empty? test) (into (sorted-map) m)
        (into (sorted-map)
              (let [index (create-index test)
                    lastindex (last (create-index (keys m)))]
                (merge m
                       (into {} (vec (reverse (zipmap
                                               (map #(nth fight-keys %) index)
                                               (repeat (count index) "N/A")))))))))))

(defn zero-to-na [x]
  (if (or (= 0 x) (= "Unknown" x)) "N/A" x))

(defn get-fighter-key [record key]
  (map zero-to-na (vals
               (validate-record (key record)))))

(defn get-fighter-info [record key]
  (map key record))

(defn fighter-index-csv [path-to-data]
  (let [all-fighter (map #(get-fighter-key % :Bio) (get-records path-to-data))]
    (map vec
         (map flatten
              (zipmap (vec (range (count all-fighter))) all-fighter)))))

;; Exec creation of fighter csv:
;; (write-fighter-csv "./resources/results.json" "./fighter-index.csv")
(defn write-fighter-csv [path-in path-out]
  (with-open [out-file (io/writer path-out)]
    (csv/write-csv out-file
                   (vec
                    (cons ["id" "birthday" "class" "country" "height" "name" "weight"]
                              (fighter-index-csv path-in))))))

(defn read-fighter-csv [path-in]
  (with-open [in-file (io/reader path-in)]
  (doall
    (csv/read-csv in-file))))

(defn compare-row [x row]
  (if (= x (:name row)) (:id row)))

(defn create-fighter-map [csv-seq]
  (map #(select-keys % [:id :name]) (map #(zipmap (map keyword (first csv-seq)) %) (rest csv-seq))))

(defn replace-name-id [x]
  (first (remove nil? (map #(compare-row x %) (create-fighter-map (read-fighter-csv "./fighter-index.csv"))))))

(defn merge-opp-id [row]
  (merge row {:op-id (replace-name-id (:Opponent row))}))

;; id,event,method,verdict,time,date,round,opponent
(defn map-rel [out-path fighter-index in-path]
  (map (create-fighter-map (read-fighter-csv fighter-index))))

(defn compress-id [row]
  (let [id-num (first row)
        fights (rest row)]
    (map merge-opp-id (map #(merge % {:id id-num}) (flatten fights)))))

(defn vectorize [row]
  (map vec row))

;; fighter-index = (create-fighter-map (read-fighter-csv "./fighter-index.xsc"))
(defn get-fight-records [path-in]
  (let [records (get-records path-in)
        index-range (range 0 (count records))
        records-seq (map vals (map #(:Fights (nth records %)) index-range))
        id-zip  (zipmap index-range records-seq)]
     (map compress-id id-zip)))

(defn get-fight-vals [fights]
  (map vals fights))

(defn write-rel-csv [path-in path-out]
  (with-open [out-file (io/writer path-out)]
    (csv/write-csv out-file
                   (vec
                    (cons ["op-id" "id" "event" "method" "verdict" "time" "date" "round" "opponent"]
                          (flatten (map vectorize (map get-fight-vals (get-fight-records path-in)))))))))
