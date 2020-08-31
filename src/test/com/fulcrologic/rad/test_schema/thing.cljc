(ns com.fulcrologic.rad.test-schema.thing
  (:require
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]
    ))

(defattr id ::id :long
  {::attr/identity?     true
   do/native-id?        true
   ::attr/schema        :production})

(defattr label ::label :string
  {::attr/schema     :production
   ::attr/identities #{::id}
   ::attr/required?  true})

(def attributes [id label])
