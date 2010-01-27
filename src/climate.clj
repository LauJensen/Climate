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
  (doall
   (flatten
    (for [file (repeatedly #(.getNextEntry tarstream))
	 :while file
	 :when (let [[_ stn wban] (re-find #"(\d+)-(\d+)" (.getName file))]
		 (and (not (.isDirectory file))
		      (or (stn-ids stn) (wban-ids wban))))]
      (for [data (rest (line-stream tarstream file))]
	{:id   (.getName file)
	 :temp (Double. (nth (cols data) 3))})))))

(defn process-tarball
  [filename stn-ids wban-ids]
  (println "Parsing: " (.getName filename)) (flush)
  (let [tarstream    (->> filename FileInputStream. TarInputStream.)
	readings     (extract-readings tarstream stn-ids wban-ids)]
    {:year  (re-find #"\d{4}" (.getName filename))
     :reads (->> (for [uid  (distinct (map :id readings))]
		   (let [temps (->> (filter #(= uid (:id %)) readings)
				    (map :temp))]
		     (if-let [cnt (count temps)]
		       (when-not (zero? cnt)
			 {:uid (->> (re-find #"(\d+)-(\d+)" uid)
				    (take 2)
				    (apply str))
			  :mean (/ (reduce + temps) cnt)}))))
		 doall)}))		 

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

(defn emit-dataset [data]
  (let [uids (distinct (flatten (map #(map :uid %) (map :reads data))))]
    (with-out-str
      (doseq [{:keys [year reads]} data]
	(print year)
	(doseq [uid uids]
	  (if-let [reading (first (filter #(= uid (:uid %)) reads))]
	    (print "," (:mean reading))
	    (print ",null")))
	(println "")))))

(defn process-weather-data
  [dataset history-file stations output]
  (let [dataset   (->> (File. dataset) file-seq (filter #(.isFile %)) sort)
	nstations (northern-stations history-file)
	stn-ids   (set (filter #((set (:stn  nstations)) %) (:stn  stations)))
	wban-ids  (set (filter #((set (:wban nstations)) %) (:wban stations)))
	headers   [:stn :wban :yearmoda :temp]
	result    (doall
		   (pmap #(process-tarball % stn-ids wban-ids) dataset))]    
    (spit output (emit-dataset result))
    (spit (str output ".raw") (with-out-str (prn result)))
    (println "Done")))