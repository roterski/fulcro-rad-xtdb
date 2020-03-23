(ns com.fulcrologic.rad.test-schema.address
  (:require
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [taoensso.timbre :as log]))

(defattr id ::id :uuid
  {::attr/identity? true
   ::attr/schema    :production})

(defattr street ::street :string
  {::attr/identities #{::id}
   ::attr/required?  true
   ::attr/schema     :production})

(def attributes [id street])
