(ns com.fulcrologic.rad.test-schema.address
  (:require
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [taoensso.timbre :as log]))

(defattr id ::id :uuid
  {::attr/identity? true
   ::datomic/schema :production})

(defattr street ::street :string
  {::datomic/entity-ids #{::id}
   ::attr/required?     true
   ::datomic/schema     :production})

(def attributes [id street])
