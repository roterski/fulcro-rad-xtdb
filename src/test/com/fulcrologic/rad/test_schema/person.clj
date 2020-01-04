(ns com.fulcrologic.rad.test-schema.person
  (:require
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [taoensso.timbre :as log]))

(defattr id ::id :uuid
  {::attr/identity? true
   ::datomic/schema :production})

(defattr email ::email :string
  {::datomic/schema     :production
   ::datomic/entity-ids #{::id}
   :db/unique           :db.unique/value
   ::attr/required?     true})

(defattr addresses ::addresses :ref
  {::attr/target              :com.fulcrologic.rad.test-schema.address/id
   ::attr/cardinality         :many
   ::datomic/schema           :production
   ::datomic/intended-targets #{:com.fulcrologic.rad.test-schema.address/id}
   ::datomic/entity-ids       #{::id}
   :db/isComponent            true})

(def attributes [id name email addresses])
