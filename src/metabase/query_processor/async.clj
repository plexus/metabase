(ns metabase.query-processor.async
  "Async versions of the usual public query processor functions. Instead of blocking while the query is ran, these
  functions all return a `core.async` channel that can be used to fetch the results when they become available.

  Each connected database is limited to a maximum of 15 simultaneous queries using these methods; any additional
  queries will park the thread. Super-useful for writing high-performance API endpoints. Prefer these methods to the
  old-school synchronous versions."
  (:require [clojure.core.async :as a]
            [metabase
             [query-processor :as qp]
             [util :as u]]))

(def ^:private max-simultaneous-db-requests 15)

(defonce ^:private db-ticket-channels (atom {}))

(defn- ticket-channel [database-or-id]
  (let [id (u/get-id database-or-id)]
    (or
     (@db-ticket-channels id)
     (let [ch     (a/chan max-simultaneous-db-requests)
           new-ch ((swap! db-ticket-channels update id #(or % ch)) id)]
       (if-not (= ch new-ch)
         (.close ch)
         (dotimes [_ max-simultaneous-db-requests]
           (a/>!! ch ::ticket)))
       new-ch))))

(defn- do-async [db f & args]
  (let [ticket-ch (ticket-channel db)
        result-ch (a/promise-chan)
        do-f      (fn [ticket]
                    (try
                      (a/put! result-ch (apply f args))
                      ;; TODO - not sure I like this. Luckily the QP doesn't really return Exceptions, it always wraps
                      ;; in those 'error' responses
                      (catch Throwable e
                        (a/put! e))
                      (finally
                        (a/close! result-ch)
                        (a/put! ticket-ch ticket))))]
    ;; Start a new go block that will call `do-f` once it acquires an exclusive ticket
    (a/go
      (let [ticket (a/<! ticket-ch)]
        (do-f ticket)))
    ;; return a channel that can be used to the a response
    result-ch))

(defn process-query [query]
  (do-async (:database query) qp/process-query query))

(defn process-query-and-save-execution! [query options]
  (do-async (:database query) qp/process-query-and-save-execution! query options))

(defn process-query-and-save-with-max! [query options]
  (do-async (:database query) qp/process-query-and-save-with-max! query options))

(defn process-query-without-save! [user query]
  (do-async (:database query) qp/process-query-without-save! user query))
