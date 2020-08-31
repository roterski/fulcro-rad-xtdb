(ns com.fulcrologic.rad.database-adapters.datomic-common-spec
  (:require
    [fulcro-spec.core :refer [specification assertions component behavior when-mocking]]
    [com.fulcrologic.rad.ids :as ids]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.test-schema.person :as person]
    [com.fulcrologic.rad.test-schema.address :as address]
    [com.fulcrologic.rad.test-schema.thing :as thing]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.datomic-common :as common]
    [fulcro-spec.core :refer [specification assertions]]
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.pathom :as pathom]
    [taoensso.timbre :as log]
    [datomic.client.api :as d]))

(declare =>)

(def all-attributes (vec (concat person/attributes address/attributes thing/attributes)))
(def key->attribute (into {}
                      (map (fn [{::attr/keys [qualified-key] :as a}]
                             [qualified-key a]))
                      all-attributes))

(def ^:dynamic *conn* nil)
(def ^:dynamic *env* {})


(defn with-env [tests]
  (binding [*env* {::attr/key->attribute key->attribute}]
    (tests)))

(use-fixtures :each with-env)

; TODO -- move to common-spec
(specification "native ID pull query transform" :focus
  (component "pathom-query->datomic-query"
    (let [incoming-query [::person/id
                          {::person/addresses [::address/id ::address/street]}
                          {::person/things [::thing/id ::thing/label]}]
          expected-datomic-query [:db/id
                                  {::person/addresses [::address/id ::address/street]}
                                  {::person/things [:db/id ::thing/label]}]
          actual-query (common/pathom-query->datomic-query all-attributes incoming-query)]
      (assertions
        "can convert a recursive pathom query to a proper Datomic query"
        actual-query => expected-datomic-query)))
  (component "datomic-result->pathom-result"
    (let [pathom-query [::person/id
                        {::person/addresses [::address/id ::address/street]}
                        {::person/things [::thing/id ::thing/label]}]
          datomic-result {:db/id             100
                          ::person/addresses [{::address/id     (ids/new-uuid 1)
                                               ::address/street "111 Main St"}]
                          ::person/things    [{:db/id        191
                                               ::thing/label "ABC"}]}
          pathom-result (common/datomic-result->pathom-result key->attribute pathom-query datomic-result)
          expected-result {::person/id        100
                           ::person/addresses [{::address/id     (ids/new-uuid 1)
                                                ::address/street "111 Main St"}]
                           ::person/things    [{::thing/id    191
                                                ::thing/label "ABC"}]}]
      (assertions
        "can convert a recursive datomic result to a proper Pathom response"
        pathom-result => expected-result))))


(specification "intermediate ID generation"
  (let [id1 (tempid/tempid (ids/new-uuid 1))
        id2 (tempid/tempid (ids/new-uuid 2))
        delta {[::person/id id1]  {::person/id        {:after id1}
                                   ::person/addresses {:after [[::address/id id2]]}}
               [::address/id id2] {::address/id     {:after id2}
                                   ::address/street {:after "111 Main St"}}}
        tmp->m (common/tempid->intermediate-id *env* delta)]
    (assertions
      "creates a map from tempid to a string"
      (get tmp->m id1) => "ffffffff-ffff-ffff-ffff-000000000001"
      (get tmp->m id2) => "ffffffff-ffff-ffff-ffff-000000000002")))

(specification "fail-safe ID"
  (let [id1 (ids/new-uuid 1)
        tid (tempid/tempid id1)]
    (assertions
      "is the Datomic :db/id when using native IDs"
      (common/failsafe-id *env* [::person/id 42]) => 42
      "is the ident when using custom IDs that are not temporary"
      (common/failsafe-id *env* [::address/id (ids/new-uuid 1)]) => [::address/id (ids/new-uuid 1)]
      "is a string-version of the tempid when the id of the ident is a tempid"
      (common/failsafe-id *env* [::person/id tid]) => "ffffffff-ffff-ffff-ffff-000000000001"
      (common/failsafe-id *env* [::address/id tid]) => "ffffffff-ffff-ffff-ffff-000000000001")))

(specification "delta->txn: simple flat delta, new entity, non-native ID. CREATE"
  (let [id1 (tempid/tempid (ids/new-uuid 1))
        expected-new-id (ids/new-uuid 2)
        str-id (str (:id id1))
        delta {[::address/id id1] {::address/id     {:after id1}
                                   ::address/street {:after "111 Main St"}}}]
    (when-mocking
      (common/next-uuid) => expected-new-id

      (let [{:keys [tempid->string txn]} (common/delta->txn *env* :production delta)]
        (assertions
          "includes tempid temporary mapping"
          (get tempid->string id1) => "ffffffff-ffff-ffff-ffff-000000000001"
          "Includes an add for the specific facts changed"
          txn => [[:db/add str-id ::address/id expected-new-id]
                  [:db/add str-id ::address/street "111 Main St"]])))))
