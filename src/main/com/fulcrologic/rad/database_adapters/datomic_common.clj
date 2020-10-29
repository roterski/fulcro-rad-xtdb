(ns com.fulcrologic.rad.database-adapters.datomic-common
  (:require
    [clojure.spec.alpha :as s]
    [clojure.set :as set]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.guardrails.core :refer [>defn => ?]]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    [com.fulcrologic.rad.ids :refer [select-keys-in-ns]]
    [com.rpl.specter :as sp]
    [edn-query-language.core :as eql]
    [taoensso.timbre :as log])
  (:import (java.util UUID)))

(def type-map
  {:string   :db.type/string
   :enum     :db.type/ref
   :boolean  :db.type/boolean
   :password :db.type/string
   :int      :db.type/long
   :long     :db.type/long
   :double   :db.type/double
   :float    :db.type/float
   :bigdec   :db.type/bigdec
   :decimal  :db.type/bigdec
   :instant  :db.type/instant
   :keyword  :db.type/keyword
   :symbol   :db.type/symbol
   :tuple    :db.type/tuple
   :ref      :db.type/ref
   :uuid     :db.type/uuid})

(>defn pathom-query->datomic-query [all-attributes pathom-query]
  [::attr/attributes ::eql/query => ::eql/query]
  (let [native-id? #(and (true? (do/native-id? %)) (true? (::attr/identity? %)))
        native-ids (set (sp/select [sp/ALL native-id? ::attr/qualified-key] all-attributes))]
    (sp/transform (sp/walker keyword?) (fn [k] (if (contains? native-ids k) :db/id k)) pathom-query)))

(defn- fix-id-keys
  "Fix the ID keys recursively on result."
  [k->a ast-nodes result]
  (let [id?                (fn [{:keys [dispatch-key]}] (some-> dispatch-key k->a ::attr/identity?))
        id-key             (:key (sp/select-first [sp/ALL id?] ast-nodes))
        join-key->children (into {}
                             (comp
                               (filter #(= :join (:type %)))
                               (map (fn [{:keys [key children]}] [key children])))
                             ast-nodes)
        join-keys          (set (keys join-key->children))
        join-key?          #(contains? join-keys %)]
    (reduce-kv
      (fn [m k v]
        (cond
          (= :db/id k) (assoc m id-key v)
          (and (join-key? k) (vector? v)) (assoc m k (mapv #(fix-id-keys k->a (join-key->children k) %) v))
          (and (join-key? k) (map? v)) (assoc m k (fix-id-keys k->a (join-key->children k) v))
          :otherwise (assoc m k v)))
      {}
      result)))

(>defn datomic-result->pathom-result
  "Convert a datomic result containing :db/id into a pathom result containing the proper id keyword that was used
   in the original query."
  [k->a pathom-query result]
  [(s/map-of keyword? ::attr/attribute) ::eql/query (? coll?) => (? coll?)]
  (when result
    (let [{:keys [children]} (eql/query->ast pathom-query)]
      (if (vector? result)
        (mapv #(fix-id-keys k->a children %) result)
        (fix-id-keys k->a children result)))))

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

(defn tempid->intermediate-id [{::attr/keys [key->attribute]} delta]
  (let [tempids (set (sp/select (sp/walker tempid/tempid?) delta))
        fulcro-tempid->real-id
                (into {} (map (fn [t] [t (str (:id t))]) tempids))]
    fulcro-tempid->real-id))

(defn native-ident?
  "Returns true if the given ident is using a database native ID (:db/id)"
  [{::attr/keys [key->attribute] :as env} ident]
  (boolean (some-> ident first key->attribute do/native-id?)))

(defn uuid-ident?
  "Returns true if the ID in the given ident uses UUIDs for ids."
  [{::attr/keys [key->attribute] :as env} ident]
  (= :uuid (some-> ident first key->attribute ::attr/type)))

(defn failsafe-id
  "Returns a fail-safe id for the given ident in a transaction. A fail-safe ID will be one of the following:
  - A long (:db/id) for a pre-existing entity.
  - A string that stands for a temporary :db/id within the transaction if the id of the ident is temporary.
  - A lookup ref (the ident itself) if the ID uses a non-native ID, and it is not a tempid.
  - A keyword if it is a keyword (a :db/ident)
  "
  [{::attr/keys [key->attribute] :as env} ident]
  (if (keyword? ident)
    ident
    (let [[_ id] ident]
      (cond
        (tempid/tempid? id) (str (:id id))
        (and (native-ident? env ident) (pos-int? id)) id
        :otherwise ident))))

(defn to-one? [{::attr/keys [key->attribute]} k]
  (not (boolean (some-> k key->attribute (attr/to-many?)))))

(defn ref? [{::attr/keys [key->attribute]} k]
  (= :ref (some-> k key->attribute ::attr/type)))

(defn schema-value? [{::attr/keys [key->attribute]} target-schema k]
  (let [{:keys       [::attr/schema]
         ::attr/keys [identity?]} (key->attribute k)]
    (and (= schema target-schema) (not identity?))))

(defn tx-value
  "Convert `v` to a transaction-safe value based on its type and cardinality."
  [env k v] (if (ref? env k) (failsafe-id env v) v))

(defn to-one-txn [env schema delta]
  (vec
    (mapcat
      (fn [[ident entity-delta]]
        (reduce
          (fn [tx [k {:keys [before after]}]]
            (if (and (schema-value? env schema k) (to-one? env k))
              (cond
                (not (nil? after)) (conj tx [:db/add (failsafe-id env ident) k (tx-value env k after)])
                (not (nil? before)) (conj tx [:db/retract (failsafe-id env ident) k (tx-value env k before)])
                :else tx)
              tx))
          []
          entity-delta))
      delta)))

(defn to-many-txn [env schema delta]
  (vec
    (mapcat
      (fn [[ident entity-delta]]
        (reduce
          (fn [tx [k {:keys [before after]}]]
            (if (and (schema-value? env schema k) (not (to-one? env k)))
              (let [before  (into #{} (map (fn [v] (tx-value env k v))) before)
                    after   (into #{} (map (fn [v] (tx-value env k v))) after)
                    adds    (map
                              (fn [v] [:db/add (failsafe-id env ident) k v])
                              (set/difference after before))
                    removes (map
                              (fn [v] [:db/retract (failsafe-id env ident) k v])
                              (set/difference before after))]
                (into tx (concat adds removes)))
              tx))
          []
          entity-delta))
      delta)))

(defn next-uuid [] (UUID/randomUUID))

(defn generate-next-id
  "Generate an id. You may pass a `suggested-id` as a UUID or a tempid. If it is a tempid and the ID column is a UUID, then
  the UUID *from* the tempid will be used. If the ID column is not a UUID then the suggested id is ignored. Returns nil for
  native ID columns."
  ([{::attr/keys [key->attribute] :as env} k]
   (generate-next-id env k (next-uuid)))
  ([{::attr/keys [key->attribute] :as env} k suggested-id]
   (let [{native-id?  :com.fulcrologic.rad.database-adapters.datomic/native-id?
          ::attr/keys [type]} (key->attribute k)]
     (cond
       native-id? nil
       (= :uuid type) (cond
                        (tempid/tempid? suggested-id) (:id suggested-id)
                        (uuid? suggested-id) suggested-id
                        :else (next-uuid))
       :otherwise (throw (ex-info "Cannot generate an ID for non-native ID attribute" {:attribute k}))))))

(defn tempids->generated-ids [{::attr/keys [key->attribute] :as env} delta]
  (let [idents (keys delta)
        fulcro-tempid->generated-id
               (into {} (keep (fn [[k id :as ident]]
                                (when (and (tempid/tempid? id) (not (native-ident? env ident)))
                                  [id (generate-next-id env k)])) idents))]
    fulcro-tempid->generated-id))

(>defn delta->txn
  [env schema delta]
  [map? keyword? map? => map?]
  (let [tempid->txid                 (tempid->intermediate-id env delta)
        tempid->generated-id         (tempids->generated-ids env delta)
        non-native-id-attributes-txn (keep
                                       (fn [[k id :as ident]]
                                         (when (and (tempid/tempid? id) (uuid-ident? env ident))
                                           [:db/add (tempid->txid id) k (tempid->generated-id id)]))
                                       (keys delta))]
    {:tempid->string       tempid->txid
     :tempid->generated-id tempid->generated-id
     :txn                  (into []
                             (concat
                               non-native-id-attributes-txn
                               (to-one-txn env schema delta)
                               (to-many-txn env schema delta)))}))

(defn- attribute-schema [attributes]
  (mapv
    (fn [{::attr/keys [identity? type qualified-key cardinality] :as a}]
      (let [attribute-schema (do/attribute-schema a)
            overrides        (select-keys-in-ns a "db")
            datomic-type     (get type-map type)]
        (when-not datomic-type
          (throw (ex-info (str "No mapping from attribute type to Datomic: " type) {})))
        (merge
          (cond-> {:db/ident       qualified-key
                   :db/cardinality (if (= :many cardinality)
                                     :db.cardinality/many
                                     :db.cardinality/one)
                   :db/valueType   datomic-type}
            (map? attribute-schema) (merge attribute-schema)
            identity? (assoc :db/unique :db.unique/identity))
          overrides)))
    attributes))

(defn- enumerated-values [attributes]
  (mapcat
    (fn [{::attr/keys [qualified-key type enumerated-values] :as a}]
      (when (= :enum type)
        (let [enum-nspc (str (namespace qualified-key) "." (name qualified-key))]
          (keep (fn [v]
                  (cond
                    (map? v) v
                    (qualified-keyword? v) {:db/ident v}
                    :otherwise (let [enum-ident (keyword enum-nspc (name v))]
                                 {:db/ident enum-ident})))
            enumerated-values))))
    attributes))

(>defn automatic-schema
  "Returns a Datomic transaction for the complete schema of the supplied RAD `attributes`
   that have a `::datomic/schema` that matches `schema-name`."
  [attributes schema-name]
  [::attr/attributes keyword? => vector?]
  (let [attributes (filter #(= schema-name (::attr/schema %)) attributes)]
    (when (empty? attributes)
      (log/warn "Automatic schema requested, but the attribute list is empty. No schema will be generated!"))
    (let [txn (attribute-schema attributes)
          txn (into txn (enumerated-values attributes))]
      txn)))