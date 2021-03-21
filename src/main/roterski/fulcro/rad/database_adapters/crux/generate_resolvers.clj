(ns roterski.fulcro.rad.database-adapters.crux.generate-resolvers
  (:require
   [com.fulcrologic.guardrails.core :refer [>defn => ?]]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.authorization :as auth]
   [roterski.fulcro.rad.database-adapters.crux-options :as co]
   [com.rpl.specter :as sp]
   [com.wsscode.pathom.connect :as pc]
   [crux.api :as crux]
   [edn-query-language.core :as eql]
   [taoensso.encore :as enc]
   [clojure.spec.alpha :as s]
   [taoensso.timbre :as log]))

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
         (= :crux.db/id k) (assoc m id-key v)
         (and (join-key? k) (vector? v)) (assoc m k (mapv #(fix-id-keys k->a (join-key->children k) %) v))
         (and (join-key? k) (map? v)) (assoc m k (fix-id-keys k->a (join-key->children k) v))
         :else (assoc m k v)))
     {}
     result)))

(>defn pathom-query->crux-query [all-attributes pathom-query]
       [::attr/attributes ::eql/query => ::eql/query]
       (let [identity? #(true? (::attr/identity? %))
             identities (set (sp/select [sp/ALL identity? ::attr/qualified-key] all-attributes))]
         (sp/transform (sp/walker keyword?) (fn [k] (if (contains? identities k) :crux.db/id k)) pathom-query)))

(>defn crux-result->pathom-result
       "Convert a crux result containing :crux.db/id into a pathom result containing the proper id keyword that was used
   in the original query."
       [k->a pathom-query result]
       [(s/map-of keyword? ::attr/attribute) ::eql/query (? coll?) => (? coll?)]
       (when result
         (let [{:keys [children]} (eql/query->ast pathom-query)]
           (if (vector? result)
             (mapv #(fix-id-keys k->a children %) result)
             (fix-id-keys k->a children result)))))


(defn get-by-ids
  [db idents _db-idents desired-output]
  (let [attr (ffirst idents)
        ids (mapv second idents)
        query {:find ['?uuid `(~'eql/project ~'?account ~desired-output)]
               :in '[[?uuid ...]]
               :where [['?account :crux.db/id '?uuid]]}
        id->value (->> (crux/q db query ids)
                       (reduce (fn [acc [id value]]
                                 (assoc acc id (assoc value attr id)))
                               {}))]
    (mapv #(get id->value %) ids)))

(defn entity-query
  [{:keys       [::attr/schema ::id-attribute]
    ::attr/keys [attributes]
    :as         env} input]
  (let [{::attr/keys [qualified-key]} id-attribute
        one? (not (sequential? input))]
    (enc/if-let [db           (some-> (get-in env [co/databases schema]) deref)
                 query        (get env ::default-query)
                 ids          (if one?
                                [(get input qualified-key)]
                                (into [] (keep #(get % qualified-key) input)))
                 ids          (mapv (fn [id] [qualified-key id]) ids)
                 enumerations (into #{}
                                    (keep #(when (= :enum (::attr/type %))
                                             (::attr/qualified-key %)))
                                    attributes)]
      (do
        (log/info "Running" query "on entities with " qualified-key ":" ids)
        (let [result (get-by-ids db ids enumerations query)]
          (if one?
            (first result)
            result)))
      (do
        (log/info "Unable to complete query.")
        nil))))

(>defn id-resolver
       "Generates a resolver from `id-attribute` to the `output-attributes`."
       [all-attributes
        {::attr/keys [qualified-key] :keys [::attr/schema ::wrap-resolve ::pc/transform] :as id-attribute}
        output-attributes]
       [::attr/attributes ::attr/attribute ::attr/attributes => ::pc/resolver]
       (log/info "Building ID resolver for" qualified-key)
       (enc/if-let [_          id-attribute
                    outputs    (attr/attributes->eql output-attributes)
                    pull-query (pathom-query->crux-query all-attributes outputs)]
         (let [resolve-sym      (symbol
                                 (str (namespace qualified-key))
                                 (str (name qualified-key) "-resolver"))
               with-resolve-sym (fn [r]
                                  (fn [env input]
                                    (r (assoc env ::pc/sym resolve-sym) input)))]
           (log/debug "Computed output is" outputs)
           (log/debug "Crux pull query to derive output is" pull-query)
           (cond-> {::pc/sym     resolve-sym
                    ::pc/output  outputs
                    ::pc/batch?  true
                    ::pc/resolve (cond-> (fn [{::attr/keys [key->attribute] :as env} input]
                                           (->> (entity-query
                                                 (assoc env
                                                        ::attr/schema schema
                                                        ::attr/attributes output-attributes
                                                        ::id-attribute id-attribute
                                                        ::default-query pull-query)
                                                 input)
                                                (crux-result->pathom-result key->attribute outputs)
                                                (auth/redact env)))
                                   wrap-resolve (wrap-resolve)
                                   :always (with-resolve-sym))
                    ::pc/input   #{qualified-key}}
             transform transform))
         (do
           (log/error "Unable to generate id-resolver. "
                      "Attribute was missing schema, or could not be found in the attribute registry: " qualified-key)
           nil)))


(defn generate-resolvers
  "Generate all of the resolvers that make sense for the given database config. This should be passed
  to your Pathom parser to register resolvers for each of your schemas."
  [attributes schema]
  (let [attributes            (filter #(= schema (::attr/schema %)) attributes)
        key->attribute        (attr/attribute-map attributes)
        entity-id->attributes (group-by ::k (mapcat (fn [attribute]
                                                      (map
                                                       (fn [id-key] (assoc attribute ::k id-key))
                                                       (get attribute ::attr/identities)))
                                                    attributes))
        entity-resolvers      (reduce-kv
                               (fn [result k v]
                                 (enc/if-let [attr     (key->attribute k)
                                              resolver (id-resolver attributes attr v)]
                                   (conj result resolver)
                                   (do
                                     (log/error "Internal error generating resolver for ID key" k)
                                     result)))
                               []
                               entity-id->attributes)]
    entity-resolvers))
