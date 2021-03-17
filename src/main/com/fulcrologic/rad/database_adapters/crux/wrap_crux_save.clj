(ns com.fulcrologic.rad.database-adapters.crux.wrap-crux-save
  (:require
   [clojure.pprint :refer [pprint]]
   [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
   [com.fulcrologic.guardrails.core :refer [>defn => ?]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.database-adapters.crux-options :as co]
   [crux.api :as c]
   [com.fulcrologic.rad.form :as form]
   [taoensso.timbre :as log])
  (:import (java.util UUID)))

(def transaction-functions
  {::delta-update '(fn [ctx [id {:keys [before after]}]]
                     (let [db (crux.api/db ctx)
                           entity (crux.api/entity db id)]
                       [[:crux.tx/match id (some-> entity (merge before))]
                        [:crux.tx/put (merge entity after)]]))})

(def keys-in-delta
  (fn keys-in-delta [delta]
    (let [id-keys  (into #{}
                         (map first)
                         (keys delta))
          all-keys (into id-keys
                         (mapcat keys)
                         (vals delta))]
      all-keys)))

(defn schemas-for-delta [{::attr/keys [key->attribute]} delta]
  (let [all-keys (keys-in-delta delta)
        schemas  (into #{}
                       (keep #(-> % key->attribute ::attr/schema))
                       all-keys)]
    schemas))

(defn ref? [{::attr/keys [key->attribute]} k]
  (= :ref (some-> k key->attribute ::attr/type)))

(defn next-uuid [] (UUID/randomUUID))

(defn generate-next-id
  "Generate an id. You may pass a `suggested-id` as a UUID or a tempid. If it is a tempid and the ID column is a UUID, then
  the UUID *from* the tempid will be used. If the ID column is not a UUID then the suggested id is ignored."
  ([{::attr/keys [key->attribute] :as env} k]
   (generate-next-id env k (next-uuid)))
  ([{::attr/keys [key->attribute] :as env} k suggested-id]
   (let [{::attr/keys [type]} (key->attribute k)]
     (cond
       (tempid/tempid? suggested-id) (:id suggested-id)
       (uuid? suggested-id) suggested-id
       :else (next-uuid)))))

(defn tempids->generated-ids [env delta]
  (->> (keys delta)
       (keep (fn [[k id]]
               (when (tempid/tempid? id)
                 [id (generate-next-id env k)])))
       (into {})))

(defn ->delta-update-txs
  [doc-delta]
  (->> doc-delta
       (reduce (fn [acc [id delta]]
                 (conj acc [:crux.tx/fn ::delta-update [id delta]]))
               [])))

(defn is-ident? [value]
  (and (vector? value)
       (= 2 (count value))
       (keyword? (first value))
       (or (uuid? (second value))
           (tempid/tempid? (second value)))))

(defn ident->id
  [[_attr key] tempid->generated-id]
  (if (tempid/tempid? key)
    (tempid->generated-id key)
    key))

(defn idents->ids
  [doc tempid->generated-id env]
  (->> doc
       (reduce (fn [acc [k v]]
                 (assoc acc k (cond (and (ref? env k)
                                         (is-ident? v)) (ident->id v tempid->generated-id)
                                    (and (ref? env k)
                                         (vector? v)
                                         (every? is-ident? v)) (mapv #(ident->id % tempid->generated-id) v)
                                    :else v)))
               {})))

(defn ->doc-delta
  "Converts attribute delta into document delta"
  [attr-delta tempid->generated-id env]
  (->> attr-delta
       (reduce (fn [d [[attr key] doc]]
                 (let [key (get tempid->generated-id key key)]
                   (assoc-in d [key] (-> (reduce (fn [acc [attr {:keys [before after]}]]
                                                   (-> acc
                                                       (assoc-in [:before attr] before)
                                                       (assoc-in [:after attr] after)))
                                                 {}
                                                 doc)
                                         (assoc-in [:after attr] key)
                                         (assoc-in [:after :crux.db/id] key)
                                         (update :before idents->ids tempid->generated-id env)
                                         (update :after idents->ids tempid->generated-id env)))))
               {})))

(>defn delta->txn
       [env schema delta]
       [map? keyword? map? => map?]
       (let [tempid->generated-id (tempids->generated-ids env delta)]
         {:tempid->generated-id tempid->generated-id
          :txn (-> delta
                   (->doc-delta tempid->generated-id env)
                   ->delta-update-txs)}))

(defn save-form!
  "Do all of the possible Crux operations for the given form delta (save to all Crux databases involved)"
  [env {::form/keys [delta]}]
  (let [schemas (schemas-for-delta env delta)
        result  (atom {:tempids {}})]
    (log/debug "Saving form across " schemas)
    (doseq [schema schemas
            :let [node (-> env co/nodes (get schema))
                  {:keys [tempid->generated-id txn]} (delta->txn env schema delta)]]
      (log/debug "Saving form delta" (with-out-str (pprint delta)))
      (log/debug "on schema" schema)
      (log/debug "Running txn\n" (with-out-str (pprint txn)))
      (if (and node (seq txn))
        (try
          (let [database-atom   (get-in env [co/databases schema])
                tx (c/submit-tx node txn)]
            (swap! result update :tempids merge tempid->generated-id)
            (c/await-tx node tx)
            (when database-atom
              (reset! database-atom (c/db node))))
          (catch Exception e
            (log/error e "Transaction failed!")
            {}))
        (log/error "Unable to save form. Either node was missing in env, or txn was empty.")))
    @result))

(defn wrap-crux-save
  "Form save middleware to accomplish Crux saves."
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [save-result (save-form! pathom-env params)]
       save-result)))
  ([handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [save-result    (save-form! pathom-env params)
           handler-result (handler pathom-env)]
       (deep-merge save-result handler-result)))))
