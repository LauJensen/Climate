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
    (->> (for [line (repeatedly #(.readLine zipfile)) :while line] line)
	 doall)))

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
  (-> (str "Parsing: " (.getName filename)) println flush)
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
   		        :wban (conj (:wban %1) (%2 1)))
	    {} north)))

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
  [dataset history-file output]
  (let [stations   (northern-stations history-file)
	;s1940      (get-station-series "/home/lau/Desktop/dataset" 1929 1940)
	;s1929      (get-stations "/home/lau/Desktop/dataset/gsod_1929.tar")
	;stn-ids    (set (filter #((set (:stn stations)) %) (:stn s1929)))
	;wban-ids   (set (filter #((set (:wban stations)) %) (:wban s1929)))
	stn-ids    (disj (set (:stn stations)) "99999")
	wban-ids   (disj (set (:wban stations)) "999999")
	dataset    (->> (File. dataset) file-seq (filter #(.isFile %)) sort)
	headers    [:stn :wban :yearmoda :temp]
	result     (->> dataset
			(pmap #(process-tarball % stn-ids wban-ids headers))
			doall)]
    (spit output (sort-by :year result))
    (println "Done")))

user> (->> [1 2 3] (map inc) | (map inc) vec (range 1 4) <<-)
[2 2 3 3 4 4]