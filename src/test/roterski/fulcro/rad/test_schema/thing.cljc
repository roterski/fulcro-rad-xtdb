(ns roterski.fulcro.rad.test-schema.thing
  (:require
   [com.fulcrologic.rad.attributes :as attr :refer [defattr]]))

(defattr id ::id :long
  {::attr/identity?     true
   ::attr/schema        :production})

(defattr label ::label :string
  {::attr/schema     :production
   ::attr/identities #{::id}
   ::attr/required?  true})

(def attributes [id label])
