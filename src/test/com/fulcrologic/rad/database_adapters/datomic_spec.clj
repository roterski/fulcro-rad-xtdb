(ns com.fulcrologic.rad.database-adapters.datomic-spec
  (:require
    [fulcro-spec.core :refer [specification assertions]]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.test-schema.person :as person]
    [com.fulcrologic.rad.test-schema.address :as address]
    [com.fulcrologic.rad.attributes :as attr]
    [fulcro-spec.core :refer [specification assertions]]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]))

(declare =>)

(def all-attributes (vec (concat person/attributes address/attributes)))

(defn with-registry [tests]
  (datomic/reset-migrated-dbs!)
  (attr/clear-registry!)
  (attr/register-attributes! person/attributes)
  (attr/register-attributes! address/attributes)
  (tests)
  (attr/clear-registry!))

(use-fixtures :once with-registry)

(specification "Saving an entity with tempids"
  (let [conn             (datomic/empty-db-connection all-attributes :production)
        tpid             (tempid/tempid)
        taid             (tempid/tempid)
        new-entity-delta {[::person/id tpid]  {::person/id        {:after tpid}
                                               ::person/email     {:after "test@example.com"}
                                               ::person/addresses {:after [[::address/id taid]]}}
                          [::address/id taid] {::address/id     {:after taid}
                                               ::address/street "111 Main St"}}
        env              {::datomic/connections {:production conn}}
        result           (datomic/save-form! env {::form/delta new-entity-delta})]
    (assertions
      "Returns proper tempid remapping for person"
      (contains? result tpid) => true
      (uuid? (get result tpid)) => true
      "Returns proper tempid remapping for address"
      (contains? result taid) => true
      (uuid? (get result taid)) => true)))
