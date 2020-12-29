(ns com.fulcrologic.rad.database-adapters.datomic-spec
  (:require
    [fulcro-spec.core :refer [specification assertions component behavior when-mocking]]
    [com.fulcrologic.rad.ids :as ids]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.test-schema.person :as person]
    [com.fulcrologic.rad.test-schema.address :as address]
    [com.fulcrologic.rad.test-schema.thing :as thing]
    [com.fulcrologic.rad.attributes :as attr]
    [fulcro-spec.core :refer [specification assertions]]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [com.fulcrologic.rad.database-adapters.datomic-common :as common]
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.pathom :as pathom]
    [taoensso.timbre :as log]
    [datomic.api :as d]
    [clojure.set :as set]
    [com.fulcrologic.rad.database-adapters.datomic-options :as do]))

(declare =>)

(def all-attributes (vec (concat person/attributes address/attributes thing/attributes)))
(def key->attribute (into {}
                      (map (fn [{::attr/keys [qualified-key] :as a}]
                             [qualified-key a]))
                      all-attributes))
(def ^:dynamic *conn* nil)
()
(def ^:dynamic *env* {})

(defn with-reset-database [tests]
  (datomic/reset-migrated-dbs!)
  (tests))

(defn with-env [tests]
  (let [conn (datomic/empty-db-connection all-attributes :production)]
    (binding [*conn* conn
              *env* {::attr/key->attribute key->attribute
                     do/connections        {:production conn}}]
      (tests))))

(use-fixtures :once with-reset-database)
(use-fixtures :each with-env)

(defn runnable? [txn]
  (try
    @(d/transact *conn* txn)
    true
    (catch Exception e
      (.getMessage e))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; To-one
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "delta->txn: simple flat delta, existing entity, non-native ID. UPDATE to-one"
  (let [id (ids/new-uuid 1)
        _ @(d/transact *conn* [{::address/id     (ids/new-uuid 1)
                                ::address/street "111 Main"}])
        delta {[::address/id id] {::address/id     {:before id :after id}
                                  ::address/street {:before "111 Main" :after "111 Main St"}}}]
    (let [{:keys [tempid->string txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "has no tempid mappings"
        (empty? tempid->string) => true
        "Includes lookup refs for the non-native ID, and changes for the facts that changed"
        txn => [[:db/add [::address/id id] ::address/street "111 Main St"]]
        (runnable? txn) => true)))
  (let [id (ids/new-uuid 1)
        delta {[::address/id id] {::address/id       {:before id :after id}
                                  ::address/enabled? {:before nil :after false}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "setting boolean false does an add"
        txn => [[:db/add
                 [:com.fulcrologic.rad.test-schema.address/id
                  #uuid "ffffffff-ffff-ffff-ffff-000000000001"]
                 :com.fulcrologic.rad.test-schema.address/enabled?
                 false]])))
  (let [id (ids/new-uuid 1)
        delta {[::address/id id] {::address/id       {:before id :after id}
                                  ::address/enabled? {:before false :after nil}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "removing boolean false does a retract"
        txn => [[:db/retract
                 [:com.fulcrologic.rad.test-schema.address/id
                  #uuid "ffffffff-ffff-ffff-ffff-000000000001"]
                 :com.fulcrologic.rad.test-schema.address/enabled?
                 false]]))))

(specification "delta->txn: simple flat delta, existing entity, non-native ID. ADD to-one ATTRIBUTE"
  (let [id (ids/new-uuid 1)
        _ @(d/transact *conn* [{::address/id (ids/new-uuid 1)}])
        delta {[::address/id id] {::address/street {:after "111 Main St"}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "Includes lookup refs for the non-native ID, and changes for the facts that changed"
        txn => [[:db/add [::address/id id] ::address/street "111 Main St"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, non-native ID. DELETE to-one ATTRIBUTE"
  (let [id (ids/new-uuid 1)
        _ @(d/transact *conn* [{::address/id     (ids/new-uuid 1)
                                ::address/street "111 Main"}])
        delta {[::address/id id] {::address/id     {:before id :after id}
                                  ::address/street {:before "111 Main" :after nil}}}]
    (let [{:keys [tempid->string txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "has no tempid mappings"
        (empty? tempid->string) => true
        "Includes lookup refs for the non-native ID, and changes for the facts that changed"
        txn => [[:db/retract [::address/id id] ::address/street "111 Main"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, new entity, native ID"
  (let [id1 (tempid/tempid (ids/new-uuid 1))
        str-id (str (:id id1))
        delta {[::person/id id1] {::person/id    {:after id1}
                                  ::person/email {:after "joe@nowhere.com"}}}]
    (let [{:keys [tempid->string txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "includes tempid temporary mapping"
        (get tempid->string id1) => "ffffffff-ffff-ffff-ffff-000000000001"
        "Includes an add for the specific facts changed"
        txn => [[:db/add str-id ::person/email "joe@nowhere.com"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, native ID, ADD attribute"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"}])
        delta {[::person/id id] {::person/email {:after "joe@nowhere.net"}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/add id ::person/email "joe@nowhere.net"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, native ID, UPDATE attribute"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"}])
        delta {[::person/id id] {::person/full-name {:before "Bob" :after "Bobby"}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/add id ::person/full-name "Bobby"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, native ID, DELETE attribute"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"}])
        delta {[::person/id id] {::person/full-name {:before "Bob"}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/retract id ::person/full-name "Bob"]]
        "Is a runnable txn"
        (runnable? txn) => true))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; To-one Enumerations
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "delta->txn: simple flat delta, existing entity, native ID, ADD to-one enum"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"}])
        delta {[::person/id id] {::person/role {:after :com.fulcrologic.rad.test-schema.person.role/admin}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/add id ::person/role :com.fulcrologic.rad.test-schema.person.role/admin]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, native ID, REMOVE to-one enum"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/role      :com.fulcrologic.rad.test-schema.person.role/admin
                                                      ::person/full-name "Bob"}])
        delta {[::person/id id] {::person/role {:before :com.fulcrologic.rad.test-schema.person.role/admin}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/retract id ::person/role :com.fulcrologic.rad.test-schema.person.role/admin]]
        (runnable? txn) => true))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; To-many Enumerations
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "delta->txn: simple flat delta, existing entity, non-native ID, UPDATE (ADD) to-many enum"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id               "id"
                                                      ::person/permissions [:com.fulcrologic.rad.test-schema.person.permissions/read
                                                                            :com.fulcrologic.rad.test-schema.person.permissions/write]}])
        delta {[::person/id id] {::person/permissions {:before [:com.fulcrologic.rad.test-schema.person.permissions/read
                                                                :com.fulcrologic.rad.test-schema.person.permissions/write]
                                                       :after  [:com.fulcrologic.rad.test-schema.person.permissions/read
                                                                :com.fulcrologic.rad.test-schema.person.permissions/execute
                                                                :com.fulcrologic.rad.test-schema.person.permissions/write]}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/add id ::person/permissions :com.fulcrologic.rad.test-schema.person.permissions/execute]]
        (runnable? txn) => true))))


(specification "delta->txn: simple flat delta, existing entity, non-native ID, UPDATE (add/remove) to-many enum"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id               "id"
                                                      ::person/permissions [:com.fulcrologic.rad.test-schema.person.permissions/read
                                                                            :com.fulcrologic.rad.test-schema.person.permissions/write]}])
        delta {[::person/id id] {::person/permissions {:before [:com.fulcrologic.rad.test-schema.person.permissions/read
                                                                :com.fulcrologic.rad.test-schema.person.permissions/write]
                                                       :after  [:com.fulcrologic.rad.test-schema.person.permissions/execute]}}}]
    (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        (set txn) => #{[:db/add id ::person/permissions :com.fulcrologic.rad.test-schema.person.permissions/execute]
                       [:db/retract id ::person/permissions :com.fulcrologic.rad.test-schema.person.permissions/write]
                       [:db/retract id ::person/permissions :com.fulcrologic.rad.test-schema.person.permissions/read]}
        (runnable? txn) => true))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; References to entities
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "Completely new entity with references to new children"
  (let [tempid1 (tempid/tempid (ids/new-uuid 1))
        tempid2 (tempid/tempid (ids/new-uuid 2))
        tempid3 (tempid/tempid (ids/new-uuid 3))
        new-address-id1 (ids/new-uuid 1)
        new-address-id2 (ids/new-uuid 2)
        sid1 "ffffffff-ffff-ffff-ffff-000000000001"
        sid2 "ffffffff-ffff-ffff-ffff-000000000002"
        sid3 "ffffffff-ffff-ffff-ffff-000000000003"
        delta {[::person/id tempid1]  {::person/id              {:after tempid1}
                                       ::person/full-name       {:after "Tony"}
                                       ::person/primary-address {:after [::address/id tempid2]}
                                       ::person/addresses       {:after [[::address/id tempid2] [::address/id tempid3]]}}
               [::address/id tempid2] {::address/id     {:after tempid2}
                                       ::address/street {:after "A St"}}
               [::address/id tempid3] {::address/id     {:after tempid3}
                                       ::address/street {:after "B St"}}}

        expected #{[:db/add sid2 :com.fulcrologic.rad.test-schema.address/id new-address-id1]
                   [:db/add sid3 :com.fulcrologic.rad.test-schema.address/id new-address-id2]
                   [:db/add sid1 :com.fulcrologic.rad.test-schema.person/full-name "Tony"]
                   [:db/add sid2 :com.fulcrologic.rad.test-schema.address/street "A St"]
                   [:db/add sid3 :com.fulcrologic.rad.test-schema.address/street "B St"]
                   [:db/add sid1 :com.fulcrologic.rad.test-schema.person/primary-address sid2]
                   [:db/add sid1 :com.fulcrologic.rad.test-schema.person/addresses sid3]
                   [:db/add sid1 :com.fulcrologic.rad.test-schema.person/addresses sid2]}]
    (when-mocking
      (common/next-uuid) =1x=> new-address-id1
      (common/next-uuid) =1x=> new-address-id2
      (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
        (assertions
          "Adds the non-native IDs, and the proper values"
          (set/difference (set txn) expected) => #{}
          (set txn) => expected
          (runnable? txn) => true)))))

(specification "Existing entity, add new to-one child"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"
                                                      ::person/addresses [{::address/id     (ids/new-uuid 1)
                                                                           ::address/street "A St"}]}])
        tempid1 (tempid/tempid (ids/new-uuid 1))
        new-address-id1 (ids/new-uuid 1)
        sid1 "ffffffff-ffff-ffff-ffff-000000000001"
        delta {[::person/id id]       {::person/primary-address {:after [::address/id tempid1]}}
               [::address/id tempid1] {::address/id     {:after tempid1}
                                       ::address/street {:after "B St"}}}]
    (when-mocking
      (common/next-uuid) =1x=> new-address-id1
      (let [{:keys [txn]} (common/delta->txn *env* :production delta)]
        (assertions
          "Adds the non-native IDs, and the proper values"
          (set txn) => #{[:db/add sid1 :com.fulcrologic.rad.test-schema.address/id new-address-id1]
                         [:db/add id :com.fulcrologic.rad.test-schema.person/primary-address sid1]
                         [:db/add sid1 ::address/street "B St"]}
          (runnable? txn) => true)))))

(specification "Existing entity, add new to-many child"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"
                                                      ::person/addresses [{::address/id     (ids/new-uuid 1)
                                                                           ::address/street "A St"}]}])
        tempid1 (tempid/tempid (ids/new-uuid 1))
        new-address-id1 (ids/new-uuid 1)
        sid1 "ffffffff-ffff-ffff-ffff-000000000001"
        delta {[::person/id id]       {::person/addresses {:before [[::address/id (ids/new-uuid 1)]]
                                                           :after  [[::address/id (ids/new-uuid 1)] [::address/id tempid1]]}}
               [::address/id tempid1] {::address/id     {:after tempid1}
                                       ::address/street {:after "B St"}}}]
    (when-mocking
      (common/next-uuid) =1x=> new-address-id1

      (let [{:keys [tempid->string txn]} (common/delta->txn *env* :production delta)]
        (assertions
          "Includes remappings for new entities"
          tempid->string => {tempid1 sid1}
          "Adds the non-native IDs, and the proper values"
          (set txn) => #{[:db/add sid1 :com.fulcrologic.rad.test-schema.address/id new-address-id1]
                         [:db/add id :com.fulcrologic.rad.test-schema.person/addresses sid1]
                         [:db/add sid1 ::address/street "B St"]}
          (runnable? txn) => true)))))

(specification "Existing entity, change to-many children"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"
                                                      ::person/addresses [{::address/id     (ids/new-uuid 1)
                                                                           ::address/street "A St"}]}])
        tempid1 (tempid/tempid (ids/new-uuid 100))
        new-address-id1 (ids/new-uuid 100)
        sid1 "ffffffff-ffff-ffff-ffff-000000000100"
        delta {[::person/id id]       {::person/addresses {:before [[::address/id (ids/new-uuid 1)]]
                                                           :after  [[::address/id tempid1]]}}
               [::address/id tempid1] {::address/id     {:after tempid1}
                                       ::address/street {:after "B St"}}}]
    (when-mocking
      (common/next-uuid) =1x=> new-address-id1

      (let [{:keys [tempid->string txn]} (common/delta->txn *env* :production delta)]
        (assertions
          "Includes remappings for new entities"
          tempid->string => {tempid1 sid1}
          "Adds the non-native IDs, and the proper values"
          (set txn) => #{[:db/add sid1 :com.fulcrologic.rad.test-schema.address/id new-address-id1]
                         [:db/add id :com.fulcrologic.rad.test-schema.person/addresses sid1]
                         [:db/retract id :com.fulcrologic.rad.test-schema.person/addresses [::address/id (ids/new-uuid 1)]]
                         [:db/add sid1 ::address/street "B St"]}
          (runnable? txn) => true)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Save Form Integration
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "save-form!"
  (let [_ @(d/transact *conn* [{::address/id (ids/new-uuid 1) ::address/street "A St"}])
        tempid1 (tempid/tempid (ids/new-uuid 100))
        delta {[::person/id tempid1]           {::person/id              tempid1
                                                ::person/full-name       {:after "Bob"}
                                                ::person/primary-address {:after [::address/id (ids/new-uuid 1)]}
                                                ::person/role            :com.fulcrologic.rad.test-schema.person.role/admin}
               [::address/id (ids/new-uuid 1)] {::address/street {:before "A St" :after "A1 St"}}}]
    (let [{:keys [tempids]} (datomic/save-form! *env* {::form/delta delta})
          real-id (get tempids tempid1)
          person (d/pull (d/db *conn*) '[::person/full-name {::person/primary-address [::address/street]}] real-id)]
      (assertions
        "Gives a proper remapping"
        (pos-int? (get tempids tempid1)) => true
        "Updates the db"
        person => {::person/full-name       "Bob"
                   ::person/primary-address {::address/street "A1 St"}}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TEMPID remapping
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "save-form! tempid remapping"
  (let [tempid1 (tempid/tempid (ids/new-uuid 100))
        tempid2 (tempid/tempid (ids/new-uuid 101))
        delta {[::person/id tempid1]  {::person/id              tempid1
                                       ::person/primary-address {:after [::address/id tempid2]}}
               [::address/id tempid2] {::address/street {:after "A1 St"}}}]
    (let [{:keys [tempids]} (datomic/save-form! *env* {::form/delta delta})]
      (assertions
        "Returns a native ID remap for entity using native IDs"
        (pos-int? (get tempids tempid1)) => true
        "Returns a non-native ID remap for entity using uuid IDs"
        (uuid? (get tempids tempid2)) => true))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Round-trip tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "Pathom parser integration (save + generated resolvers)"
  (let [save-middleware (datomic/wrap-datomic-save)
        delete-middleware (datomic/wrap-datomic-delete)
        automatic-resolvers (datomic/generate-resolvers all-attributes :production)
        parser (pathom/new-parser {}
                 [(attr/pathom-plugin all-attributes)
                  (form/pathom-plugin save-middleware delete-middleware)
                  (datomic/pathom-plugin (fn [env] {:production *conn*}))]
                 [automatic-resolvers form/resolvers])]
    (component "Saving new items (native ID)"
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
          (pos-int? real-id) => true
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
                    ::person/role            {:after :com.fulcrologic.rad.test-schema.person.role/admin}
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
                     ::person/role            :com.fulcrologic.rad.test-schema.person.role/admin
                     ::person/primary-address {::address/id     addr-id
                                               ::address/street "A St"}})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Attr Options Tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "Attribute Options"
  (let [person-resolver (first (datomic/generate-resolvers person/attributes :production))]
    (component "defattr applies ::pc/transform to the resolver map"
      (assertions
        "person resolver has been transformed by ::pc/transform"
        (do
          (log/spy :info person-resolver)
          (::person/transform-succeeded person-resolver)) => true
        ))))
