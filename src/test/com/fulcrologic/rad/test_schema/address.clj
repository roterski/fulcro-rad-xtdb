(ns com.fulcrologic.rad.test-schema.address
  (:require
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]))

(defattr id ::id :uuid
  {::attr/identity? true
   ::attr/schema    :production})

(defattr enabled? ::enabled? :boolean
  {::attr/identities #{::id}
   ::attr/schema     :production})

(defattr street ::street :string
  {::attr/identities #{::id}
   ::attr/required?  true
   ::attr/schema     :production})

(def attributes [id enabled? street])
