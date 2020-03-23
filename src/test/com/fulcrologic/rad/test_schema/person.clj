(ns com.fulcrologic.rad.test-schema.person
  (:require
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [taoensso.timbre :as log]))

(defattr id ::id :long
  {::attr/identity?     true
   ::datomic/native-id? true
   ::datomic/schema     :production})

(defattr full-name ::full-name :string
  {::datomic/schema     :production
   ::datomic/entity-ids #{::id}
   ::attr/required?     true})

(defattr role ::role :enum
  {::datomic/schema         :production
   ::datomic/entity-ids     #{::id}
   ::attr/enumerated-values #{:user :admin}
   ::attr/cardinality       :one})

(defattr permissions ::permissions :enum
  {::datomic/schema         :production
   ::datomic/entity-ids     #{::id}
   ::attr/enumerated-values #{:read :write :execute}
   ::attr/cardinality       :many})

(defattr email ::email :string
  {::datomic/schema     :production
   ::datomic/entity-ids #{::id}
   ::attr/required?     true})

(defattr primary-address ::primary-address :ref
  {::attr/target        :com.fulcrologic.rad.test-schema.address/id
   ::datomic/schema     :production
   ::datomic/entity-ids #{::id}})

(defattr addresses ::addresses :ref
  {::attr/target        :com.fulcrologic.rad.test-schema.address/id
   ::attr/cardinality   :many
   ::datomic/schema     :production
   ::datomic/entity-ids #{::id}})

(defattr things ::things :ref
  {::attr/target        :com.fulcrologic.rad.test-schema.thing/id
   ::attr/cardinality   :many
   ::datomic/schema     :production
   ::datomic/entity-ids #{::id}})

(def attributes [id full-name email primary-address addresses role permissions])
