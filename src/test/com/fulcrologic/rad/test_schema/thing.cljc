(ns com.fulcrologic.rad.test-schema.thing
  (:require
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]))

(defattr id ::id :long
  {::attr/identity?     true
   ::datomic/native-id? true
   ::attr/schema        :production})

(defattr label ::label :string
  {::attr/schema     :production
   ::attr/identities #{::id}
   ::attr/required?  true})

(def attributes [id label])
