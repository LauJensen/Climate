(ns climate
  (:import (java.io File InputStreamReader BufferedReader FileInputStream
		     ByteArrayInputStream)
	   (java.util.zip GZIPInputStream)
	   (com.ice.tar TarArchive TarInputStream))
  (:use    (clojure.contrib duck-streams seq-utils)))

(defn dump-stream [stream sz]
  (let [buffer    (make-array Byte/TYPE sz)]
    (.read stream buffer 0 sz)
    (ByteArrayInputStream. buffer)))

(defn line-stream
  [tarstream tarentry]
  (with-open [zipfile (->> (dump-stream tarstream (.getSize tarentry))
			   GZIPInputStream. InputStreamReader. BufferedReader.)]
    (doall (for [line (repeatedly #(.readLine zipfile)) :while line] line))))

(defn cols [strdata]
  (->> (.split strdata " ") (remove empty?)))

(defn extract-readings
  [tarstream stn-ids wban-ids]
  (->> (Double. (nth (cols data) 3))
       (for [data (rest (line-stream tarstream file))])
       (for [file (repeatedly #(.getNextEntry tarstream))
	     :while file
	     :when (let [[_ stn wban] (re-find #"(\d+)-(\d+)" (.getName file))]
		     (and (not (.isDirectory file))
			  (or (stn-ids stn) (wban-ids wban))))])
       flatten))

(defn process-tarball
  [filename stn-ids wban-ids]
  (println "Parsing: " (.getName filename)) (flush)
  (let [tarstream    (->> filename FileInputStream. TarInputStream.)
	readings     (extract-readings tarstream stn-ids wban-ids)]
    {:year (re-find #"\d{4}" (.getName filename))
     :mean (if-let [cnt (count readings)]
	     (when-not (zero? cnt)
	       (/ (reduce + readings) cnt)))}))

(defn northern-stations [filename]
  (println "Generating list of stations from the northern hemisphere...")
  (let [data   (->> (line-seq (reader filename)) (drop 20))
	north  (for [station data :when (= \+ (nth station 58))]
		 (vec (take 2 (.split station " "))))]
    (reduce #(assoc %1  :stn  (conj (:stn %1) (%2 0))
		    :wban (conj (:wban %1) (%2 1))) {} north)))

(defn get-stations [filename]
  (let [tarstream    (->> filename FileInputStream. TarInputStream.)
        all-stations (for [file (repeatedly #(.getNextEntry tarstream))
			   :while file
			   :when (not (.isDirectory file))]
		       (let [[_ stn wban] (re-find #"(\d+)-(\d+)-" (.getName file))]
			 {:stn  stn :wban wban}))]
    {:stn  (disj (set (map :stn all-stations)) "99999")
     :wban (disj (set (map :wban all-stations)) "99999")}))

(defn get-station-series [base start end]
  (apply merge-with into
    (for [i (range start (inc end))]
      (get-stations
       (str base (if (not= \/ (last base)) "/") "gsod_" i ".tar")))))

(defn process-weather-data
  [dataset history-file stations output]
  (let [dataset   (->> (File. dataset) file-seq (filter #(.isFile %)) sort)
	nstations (northern-stations history-file)
	stn-ids   (set (filter #((set (:stn  nstations)) %) (:stn  stations)))
	wban-ids  (set (filter #((set (:wban nstations)) %) (:wban stations)))
	headers   [:stn :wban :yearmoda :temp]
	result    (->> dataset
		       (pmap #(process-tarball % stn-ids wban-ids))
		       doall)
	low-year  (remove #(nil? (:mean %)) (sort-by :mean result))
	high-year (last low-year)
	low-year  (first low-year)]
      (println (format "Stations tracked: %s" (+ (count stn-ids)
						 (count wban-ids))))
      (println (format "Lowest temperature: %s (%s)"
		       (:year low-year)(:mean low-year)))
      (println (format "Warmest temperature: %s (%s)"
		       (:year high-year)(:mean high-year))))
  (println "Done"))

(let [tracked-stations  (get-stations "res/dataset/gsod_1929.tar")]
;      tracked-stations  (get-station-series "res/dataset" 1929 1940)]
  (process-weather-data "res/dataset/" "res/history" tracked-stations
       "stations-from-1929-2"))