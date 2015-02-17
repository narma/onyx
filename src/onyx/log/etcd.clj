(ns onyx.log.etcd
  (:require [clojure.core.async :refer [timeout go-loop chan >!! <!! <! close! thread put! alts!]]
            [clojure.data.fressian :as fressian]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal warn]]
            [etcd-clj.core :as etcd]
            [clojure.data.codec.base64 :as b64]
            [onyx.extensions :as extensions]))

(def root-path "onyx")

(defn prefix-path [prefix]
  (str root-path "/" prefix))

(defn pulse-path [prefix]
  (str (prefix-path prefix) "/pulse"))

(defn log-path [prefix]
  (str (prefix-path prefix) "/log"))

(defn catalog-path [prefix]
  (str (prefix-path prefix) "/catalog"))

(defn workflow-path [prefix]
  (str (prefix-path prefix) "/workflow"))

(defn task-path [prefix]
  (str (prefix-path prefix) "/task"))

(defn sentinel-path [prefix]
  (str (prefix-path prefix) "/sentinel"))

(defn origin-path [prefix]
  (str (prefix-path prefix) "/origin"))

(defn job-scheduler-path [prefix]
  (str (prefix-path prefix) "/job-scheduler"))

(defn serialize [x]
  (-> x
      fressian/write
      (.array)
      b64/encode
      (String. "UTF-8")))

(defn deserialize [x]
  (some-> x
      (.getBytes "UTF-8")
      b64/decode
      fressian/read))


(defn initialize-origin! [config prefix]
  (let [node (str (origin-path prefix) "/origin")
        bytes (serialize {:message-id 1 :replica {}})]
    (etcd/set node bytes)))


(defn etcd-subscribe [k cb & {:keys [recursive
                                     start-index]
                              :or {start-index 1}}]
  (let [ch (chan)
        exit-chan (chan)]
    (go-loop [wait-index start-index]
      (let [f (etcd/wait k
                         :recursive recursive
                         :wait-index wait-index
                         :callback (fn [newval]
                                      (put! ch (cb newval))))
            [continue? ch] (alts! [ch exit-chan])]
      (if (= ch exit-chan)
        (future-cancel f)
        (when continue?
          (recur (-> @f :node :modifiedIndex inc))))))
    exit-chan))

(defrecord Etcd [config]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Etcd")
    (let [onyx-id (:onyx/id config)]
      ; (when-let [etcd-config (:etcd/address config)]
                                        ;   (etcd/connect! etcd-config))
      (etcd/set-default-timeout! 20000)
      (etcd/mkdir root-path)
      (etcd/mkdir (prefix-path onyx-id))
      (etcd/mkdir (pulse-path onyx-id))
      (etcd/mkdir (log-path onyx-id))
      (etcd/mkdir (catalog-path onyx-id))
      (etcd/mkdir (workflow-path onyx-id))
      (etcd/mkdir (task-path onyx-id))
      (etcd/mkdir (sentinel-path onyx-id))
      (etcd/mkdir (origin-path onyx-id))
      (etcd/mkdir (job-scheduler-path onyx-id))

      (initialize-origin! config onyx-id)
      (assoc component :subscribers (atom []) :wait-futures (atom []) :prefix onyx-id)))

  (stop [component]
    (doseq [subscriber @(:subscribers component)]
      (close! subscriber))

    (doseq [f @(:wait-futures component)]
      (future-cancel f))

    component))

(defn etcd [config]
  (map->Etcd {:config config}))


(defmethod extensions/write-log-entry Etcd
  [{:keys [opts prefix] :as log} data]
  (let [node (log-path prefix)
        bytes (serialize data)]
    (etcd/set node bytes :order true)))

(defmethod extensions/read-log-entry Etcd
  [{:keys [opts prefix] :as log} position]
  (let [node (str (log-path prefix) "/" position)
        data (etcd/sget node)
        content (deserialize data)]
    (when (nil? content)
      (taoensso.timbre/error "Try to read deleted or not existing log " position)
      (throw (ex-info "Log not found" {:position position})))
    (assoc content :message-id position :created-at (:ctime (:stat data)))))

(defmethod extensions/register-pulse Etcd
  [{:keys [opts prefix] :as log} id]
  (let [node (str (pulse-path prefix) "/" id)]
    (etcd/set node id :ttl 5)
    (go-loop []
      (let [resp (etcd/set node id :ttl 5 :prev-exist true)]
        (<! (timeout 4000))
        (when-not (contains? resp :errorCode)
          (recur))))))

(defmethod extensions/on-delete Etcd
  [{:keys [opts prefix subscribers] :as log} id ch]
  (let [f (fn [newvalue]
            (if (nil? (-> newvalue :node :value))
              (do
                (>!! ch true)
                false)
              true))
        path (str (pulse-path prefix) "/" id)
        pulse (etcd/get path)]
    (try
      (if-not (-> pulse :node :value)
        (>!! ch true)
        (swap! subscribers
               conj (etcd-subscribe path f
                               :start-index (-> pulse :node :modifiedIndex inc)
                               )))
      (catch Exception e
        ;; Node doesn't exist.
        (>!! ch true)))))

(defn find-job-scheduler [log]
  (loop []
    (if-let [chunk
             (try
               (extensions/read-chunk log :job-scheduler nil)
               (catch Exception e
                 (warn e)
                 (warn "Job scheduler couldn't be discovered. Backing off 500ms and trying again...")
                 nil))]
      (:job-scheduler chunk)
      (do (Thread/sleep 500)
          (recur)))))

(defmethod extensions/subscribe-to-log Etcd
  [{:keys [opts prefix subscribers] :as log} ch]
  (try
    (let [job-scheduler (find-job-scheduler log)
          origin (extensions/read-chunk log :origin nil)]
      (swap! subscribers conj
             (etcd-subscribe (log-path prefix) 
                             (fn [value] (>!! ch (-> value :node :createdIndex)) true)
                             :start-index (-> (etcd/get (log-path prefix))
                                              :node :modifiedIndex inc)
                             :recursive true))
      (assoc (:replica origin) :job-scheduler job-scheduler))
    (catch Exception e
      (fatal e))))
         

(defmethod extensions/write-chunk [Etcd :catalog]
  [{:keys [opts prefix] :as log} kw chunk id]
  (let [node (str (catalog-path prefix) "/" id)
        bytes (serialize chunk)]
    (etcd/set node bytes)))

(defmethod extensions/write-chunk [Etcd :workflow]
  [{:keys [opts prefix] :as log} kw chunk id]
  (let [node (str (workflow-path prefix) "/" id)
        bytes (serialize chunk)]
    (etcd/set node bytes)))

(defmethod extensions/write-chunk [Etcd :task]
  [{:keys [opts prefix] :as log} kw chunk id]
  (let [node (str (task-path prefix) "/" (:id chunk))
        bytes (serialize chunk)]
    (etcd/set node bytes)))

(defmethod extensions/write-chunk [Etcd :sentinel]
  [{:keys [opts prefix] :as log} kw chunk id]
  (let [node (str (sentinel-path prefix) "/" id)
        bytes (serialize chunk)]
    (etcd/set node bytes)))

(defmethod extensions/write-chunk [Etcd :job-scheduler]
  [{:keys [opts prefix] :as log} kw chunk id]
  (let [node (str (job-scheduler-path prefix) "/scheduler")
        bytes (serialize chunk)]
    (etcd/set node bytes)))

(defmethod extensions/read-chunk [Etcd :catalog]
  [{:keys [opts prefix] :as log} kw id]
  (let [node (str (catalog-path prefix) "/" id)]
    (deserialize (etcd/sget node))))

(defmethod extensions/read-chunk [Etcd :workflow]
  [{:keys [opts prefix] :as log} kw id]
  (let [node (str (workflow-path prefix) "/" id)]
    (deserialize (etcd/sget node))))

(defmethod extensions/read-chunk [Etcd :task]
  [{:keys [conn opts prefix] :as log} kw id]
  (let [node (str (task-path prefix) "/" id)]
    (deserialize (etcd/sget node))))

(defmethod extensions/read-chunk [Etcd :sentinel]
  [{:keys [opts prefix] :as log} kw id]
  (let [node (str (sentinel-path prefix) "/" id)]
    (deserialize (etcd/sget node))))

(defmethod extensions/read-chunk [Etcd :origin]
  [{:keys [opts prefix] :as log} kw id]
  (let [node (str (origin-path prefix) "/origin")]
    (deserialize (etcd/sget node))))

(defmethod extensions/read-chunk [Etcd :job-scheduler]
  [{:keys [opts prefix] :as log} kw id]
  (let [node (str (job-scheduler-path prefix) "/scheduler")]
    (deserialize (etcd/sget node))))

(defmethod extensions/update-origin! Etcd
  [{:keys [opts prefix] :as log} replica message-id]
  (let [node (str (origin-path prefix) "/origin")
        version (-> (etcd/get node) :node :modifiedIndex)
        content (deserialize (etcd/sget node))]
    (when (< (:message-id content) message-id)
      (let [new-content {:message-id message-id :replica replica}
            resp (etcd/set node (serialize new-content) :prev-index version)]
        (when (contains? resp :errorCode)
          (taoensso.timbre/error "Update failed " resp)
          (throw (ex-info "update failed" resp)))
        new-content
        ))))

(defmethod extensions/gc-log-entry Etcd
  [{:keys [opts prefix] :as log} position]
  (let [node (str (log-path prefix) "/" position)]
    (etcd/del node)))
