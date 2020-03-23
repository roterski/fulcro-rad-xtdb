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
   ::attr/schema        :production})

(defattr full-name ::full-name :string
  {::attr/schema     :production
   ::attr/identities #{::id}
   ::attr/required?  true})

(defattr role ::role :enum
  {::attr/schema            :production
   ::attr/identities        #{::id}
   ::attr/enumerated-values #{:user :admin}
   ::attr/cardinality       :one})

(defattr permissions ::permissions :enum
  {::attr/schema            :production
   ::attr/identities        #{::id}
   ::attr/enumerated-values #{:read :write :execute}
   ::attr/cardinality       :many})

(defattr email ::email :string
  {::attr/schema     :production
   ::attr/identities #{::id}
   ::attr/required?  true})

(defattr primary-address ::primary-address :ref
  {::attr/target     :com.fulcrologic.rad.test-schema.address/id
   ::attr/schema     :production
   ::attr/identities #{::id}})

(defattr addresses ::addresses :ref
  {::attr/target      :com.fulcrologic.rad.test-schema.address/id
   ::attr/cardinality :many
   ::attr/schema      :production
   ::attr/identities  #{::id}})

(defattr things ::things :ref
  {::attr/target      :com.fulcrologic.rad.test-schema.thing/id
   ::attr/cardinality :many
   ::attr/schema      :production
   ::attr/identities  #{::id}})

(def attributes [id full-name email primary-address addresses role permissions])
