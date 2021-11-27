(ns roterski.fulcro.rad.database-adapters.xtdb-spec
  (:require
   [fulcro-spec.core :refer [specification assertions component behavior when-mocking =>]]
   [com.fulcrologic.rad.ids :as ids]
   [com.fulcrologic.rad.form :as form]
   [roterski.fulcro.rad.test-schema.person :as person]
   [roterski.fulcro.rad.test-schema.address :as address]
   [roterski.fulcro.rad.test-schema.thing :as thing]
   [com.fulcrologic.rad.attributes :as attr]
   [roterski.fulcro.rad.database-adapters.xtdb.wrap-xtdb-save :as wcs]
   [roterski.fulcro.rad.database-adapters.xtdb :as xtdb-adapter]
   [roterski.fulcro.rad.database-adapters.xtdb-options :as xo]
   [xtdb.api :as xt]
   [clojure.test :refer [use-fixtures]]
   [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
   [com.fulcrologic.rad.pathom :as pathom]
   [taoensso.timbre :as log]))

(def all-attributes (vec (concat person/attributes address/attributes thing/attributes)))
(def key->attribute (into {}
                          (map (fn [{::attr/keys [qualified-key] :as a}]
                                 [qualified-key a]))
                          all-attributes))
(def ^:dynamic *node* nil)
(def ^:dynamic *env* {})

(defn with-env [tests]
  (let [node (:main (xtdb-adapter/start-databases {xo/databases {:main {}}}))]
    (binding [*node* node
              *env* {::attr/key->attribute key->attribute
                     xo/nodes       {:production node}}]
      (tests)
      (.close node))))

(use-fixtures :each with-env)

(specification "save-form!"
               (let [_ (->> [[::xt/put {:xt/id (ids/new-uuid 1) ::address/id (ids/new-uuid 1) ::address/street "A St"}]]
                            (xt/submit-tx *node*)
                            (xt/await-tx *node*))
                     tempid1 (tempid/tempid (ids/new-uuid 100))
                     delta {[::person/id tempid1]           {::person/id              tempid1
                                                             ::person/full-name       {:after "Bob"}
                                                             ::person/primary-address {:after [::address/id (ids/new-uuid 1)]}
                                                             ::person/role            :roterski.fulcro.rad.test-schema.person.role/admin}
                            [::address/id (ids/new-uuid 1)] {::address/street {:before "A St" :after "A1 St"}}}
                     {:keys [tempids]} (wcs/save-form! *env* {::form/delta delta})
                     real-id (get tempids tempid1)]
                 (assertions
                  "Gives a proper remapping"
                  (uuid? (get tempids tempid1)) => true
                  "updates the existing doc"
                  (-> (xt/db *node*)
                      (xt/q '{:find [(pull ?uid [::address/street])]
                                :in [id]
                                :where [[?uid :xt/id id]]}
                              (ids/new-uuid 1))
                      ffirst) => {::address/street "A1 St"}
                  "creates a new doc"
                  (-> (xt/db *node*)

                      (xt/q '{:find [(pull ?uid [::person/full-name {::person/primary-address [::address/street]}])]
                                :in [id]
                                :where [[?uid :xt/id id]]}
                              real-id)

                      ffirst) => {::person/full-name       "Bob"
                                  ::person/primary-address {::address/street "A1 St"}})))


(specification "save-form! when there's a 'before' data mismatch"
               (let [_ (->> [[::xt/put {:xt/id (ids/new-uuid 1) ::address/enabled? true ::address/id (ids/new-uuid 1) ::address/street "A St"}]]
                            (xt/submit-tx *node*)
                            (xt/await-tx *node*))
                     tempid1 (tempid/tempid (ids/new-uuid 100))
                     delta {[::person/id tempid1]           {::person/id              tempid1
                                                             ::person/full-name       {:after "Bob"}
                                                             ::person/primary-address {:after [::address/id (ids/new-uuid 1)]}}
                            [::address/id (ids/new-uuid 1)] {::address/street {:before "A St" :after "A1 St"}
                                                             ::address/enabled? {:before false :after true}}}
                     _ (wcs/save-form! *env* {::form/delta delta})]
                 (assertions
                  "does not update the existing doc"
                  (-> (xt/db *node*)
                      (xt/q '{:find [(pull ?uid [::address/street])]
                                :in [id]
                                :where [[?uid :xt/id id]]}
                              (ids/new-uuid 1))
                      ffirst) => {::address/street "A St"}
                  "does not create a new doc"
                  (-> (xt/db *node*)
                      (xt/q '{:find [(pull ?uid [::person/full-name])]
                                :where [[?uid ::person/full-name _]]})
                      ffirst) => nil)))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; Round-trip tests
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "Pathom parser integration (save + generated resolvers)"
               (let [save-middleware (xtdb-adapter/wrap-xtdb-save)
                     delete-middleware (xtdb-adapter/wrap-xtdb-delete)
                     automatic-resolvers (xtdb-adapter/generate-resolvers all-attributes :production)
                     parser (pathom/new-parser {}
                                               [(attr/pathom-plugin all-attributes)
                                                (form/pathom-plugin save-middleware delete-middleware)
                                                (xtdb-adapter/pathom-plugin (fn [env] {:production *node*}))]
                                               [automatic-resolvers form/resolvers])]
                 (component "Saving new items"
                            (let [temp-person-id (tempid/tempid)
                                  delta {[::person/id temp-person-id] {::person/id        {:after temp-person-id}
                                                                       ::person/full-name {:after "Bob"}}}
                                  {::form/syms [save-form]} (parser {} `[{(form/save-form ~{::form/id        temp-person-id
                                                                                            ::form/master-pk ::person/id
                                                                                            ::form/delta     delta}) [:tempids ::person/id ::person/full-name]}])
                                  {:keys [tempids]} save-form
                                  real-id (get tempids temp-person-id)
                                  entity (dissoc save-form :tempids)]
                              (assertions
                               "Includes the remapped (native) ID for native id attribute"
                               (uuid? real-id) => true
                               "Returns the newly-created attributes"
                               entity => {::person/id        real-id
                                          ::person/full-name "Bob"})))
                 (component "Saving new items (generated ID)"
                            (let [temp-address-id (tempid/tempid)
                                  delta {[::address/id temp-address-id] {::address/id     {:after temp-address-id}
                                                                         ::address/street {:after "A St"}}}
                                  {::form/syms [save-form]} (parser {} `[{(form/save-form ~{::form/id        temp-address-id
                                                                                            ::form/master-pk ::address/id
                                                                                            ::form/delta     delta}) [:tempids ::address/id ::address/street]}])
                                  {:keys [tempids]} save-form
                                  real-id (get tempids temp-address-id)
                                  entity (dissoc save-form :tempids)]
                              (assertions
                               "Includes the remapped (UUID) ID for id attribute"
                               (uuid? real-id) => true
                               "Returns the newly-created attributes"
                               entity => {::address/id     real-id
                                          ::address/street "A St"})))
                 (component "Saving a tree"
                            (let [temp-person-id (tempid/tempid)
                                  temp-address-id (tempid/tempid)
                                  delta {[::person/id temp-person-id]
                                         {::person/id              {:after temp-person-id}
                                          ::person/role            {:after :roterski.fulcro.rad.test-schema.person.role/admin}
                                          ::person/primary-address {:after [::address/id temp-address-id]}}

                                         [::address/id temp-address-id]
                                         {::address/id     {:after temp-address-id}
                                          ::address/street {:after "A St"}}}
                                  {::form/syms [save-form]} (parser {} `[{(form/save-form ~{::form/id        temp-person-id
                                                                                            ::form/master-pk ::person/id
                                                                                            ::form/delta     delta})
                                                                          [:tempids ::person/id ::person/role
                                                                           {::person/primary-address [::address/id ::address/street]}]}])
                                  {:keys [tempids]} save-form
                                  addr-id (get tempids temp-address-id)
                                  person-id (get tempids temp-person-id)
                                  entity (dissoc save-form :tempids)]
                              (assertions
                               "Returns the newly-created graph"
                               entity => {::person/id              person-id
                                          ::person/role            :roterski.fulcro.rad.test-schema.person.role/admin
                                          ::person/primary-address {::address/id     addr-id
                                                                    ::address/street "A St"}})))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; Attr Options Tests
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "Attribute Options"
               (let [person-resolver (first (xtdb-adapter/generate-resolvers person/attributes :production))]
                 (component "defattr applies ::pc/transform to the resolver map"
                            (assertions
                             "person resolver has been transformed by ::pc/transform"
                             (do
                               (log/spy :info person-resolver)
                               (::person/transform-succeeded person-resolver)) => true))))

(comment
  (require 'fulcro-spec.reporters.repl)
  (fulcro-spec.reporters.repl/run-tests)
  )
