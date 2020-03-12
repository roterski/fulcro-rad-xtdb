(ns com.fulcrologic.rad.database-adapters.datomic-spec
  (:require
    [fulcro-spec.core :refer [specification assertions behavior when-mocking]]
    [com.fulcrologic.rad.ids :as ids]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.test-schema.person :as person]
    [com.fulcrologic.rad.test-schema.address :as address]
    [com.fulcrologic.rad.attributes :as attr]
    [fulcro-spec.core :refer [specification assertions]]
    [com.fulcrologic.rad.database-adapters.datomic :as datomic]
    [clojure.test :refer [use-fixtures]]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [taoensso.timbre :as log]
    [datomic.api :as d]))

(declare =>)

(def all-attributes (vec (concat person/attributes address/attributes)))
(def key->attribute (into {}
                      (map (fn [{::attr/keys [qualified-key] :as a}]
                             [qualified-key a]))
                      all-attributes))
(def ^:dynamic *conn* nil)
(def ^:dynamic *env* {})

(defn with-reset-database [tests]
  (datomic/reset-migrated-dbs!)
  (tests))

(defn with-env [tests]
  (let [conn (datomic/empty-db-connection all-attributes :production)]
    (binding [*conn* conn
              *env*  {::attr/key->attribute key->attribute
                      ::datomic/connections {:production conn}}]
      (tests))))

(use-fixtures :once with-reset-database)
(use-fixtures :each with-env)

(specification "intermediate ID generation"
  (let [id1    (tempid/tempid (ids/new-uuid 1))
        id2    (tempid/tempid (ids/new-uuid 2))
        delta  {[::person/id id1]  {::person/id        {:after id1}
                                    ::person/addresses {:after [[::address/id id2]]}}
                [::address/id id2] {::address/id     {:after id2}
                                    ::address/street {:after "111 Main St"}}}
        tmp->m (datomic/tempid->intermediate-id *env* delta)]
    (assertions
      "creates a map from tempid to a string"
      (get tmp->m id1) => "ffffffff-ffff-ffff-ffff-000000000001"
      (get tmp->m id2) => "ffffffff-ffff-ffff-ffff-000000000002")))

(specification "fail-safe ID"
  (let [id1 (ids/new-uuid 1)
        tid (tempid/tempid id1)]
    (assertions
      "is the Datomic :db/id when using native IDs"
      (datomic/failsafe-id *env* [::person/id 42]) => 42
      "is the ident when using custom IDs that are not temporary"
      (datomic/failsafe-id *env* [::address/id (ids/new-uuid 1)]) => [::address/id (ids/new-uuid 1)]
      "is a string-version of the tempid when the id of the ident is a tempid"
      (datomic/failsafe-id *env* [::person/id tid]) => "ffffffff-ffff-ffff-ffff-000000000001"
      (datomic/failsafe-id *env* [::address/id tid]) => "ffffffff-ffff-ffff-ffff-000000000001")))

(defn runnable? [txn]
  (try
    @(d/transact *conn* txn)
    true
    (catch Exception e
      (.getMessage e))))

(specification "delta->txn: simple flat delta, new entity, non-native ID. CREATE"
  (let [id1             (tempid/tempid (ids/new-uuid 1))
        expected-new-id (ids/new-uuid 2)
        str-id          (str (:id id1))
        delta           {[::address/id id1] {::address/id     {:after id1}
                                             ::address/street {:after "111 Main St"}}}]
    (when-mocking
      (datomic/next-uuid) => expected-new-id

      (let [{:keys [tempid->string txn]} (datomic/delta->txn *env* :production delta)]
        (assertions
          "includes tempid temporary mapping"
          (get tempid->string id1) => "ffffffff-ffff-ffff-ffff-000000000001"
          "Includes an add for the specific facts changed"
          txn => [[:db/add str-id ::address/id expected-new-id]
                  [:db/add str-id ::address/street "111 Main St"]])))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; To-one
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(specification "delta->txn: simple flat delta, existing entity, non-native ID. UPDATE to-one"
  (let [id    (ids/new-uuid 1)
        _     @(d/transact *conn* [{::address/id     (ids/new-uuid 1)
                                    ::address/street "111 Main"}])
        delta {[::address/id id] {::address/id     {:before id :after id}
                                  ::address/street {:before "111 Main" :after "111 Main St"}}}]
    (let [{:keys [tempid->string txn]} (datomic/delta->txn *env* :production delta)]
      (assertions
        "has no tempid mappings"
        (empty? tempid->string) => true
        "Includes lookup refs for the non-native ID, and changes for the facts that changed"
        txn => [[:db/add [::address/id id] ::address/street "111 Main St"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, non-native ID. ADD to-one ATTRIBUTE"
  (let [id    (ids/new-uuid 1)
        _     @(d/transact *conn* [{::address/id (ids/new-uuid 1)}])
        delta {[::address/id id] {::address/street {:after "111 Main St"}}}]
    (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
      (assertions
        "Includes lookup refs for the non-native ID, and changes for the facts that changed"
        txn => [[:db/add [::address/id id] ::address/street "111 Main St"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, non-native ID. DELETE to-one ATTRIBUTE"
  (let [id    (ids/new-uuid 1)
        _     @(d/transact *conn* [{::address/id     (ids/new-uuid 1)
                                    ::address/street "111 Main"}])
        delta {[::address/id id] {::address/id     {:before id :after id}
                                  ::address/street {:before "111 Main" :after nil}}}]
    (let [{:keys [tempid->string txn]} (datomic/delta->txn *env* :production delta)]
      (assertions
        "has no tempid mappings"
        (empty? tempid->string) => true
        "Includes lookup refs for the non-native ID, and changes for the facts that changed"
        txn => [[:db/retract [::address/id id] ::address/street "111 Main"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, new entity, native ID"
  (let [id1    (tempid/tempid (ids/new-uuid 1))
        str-id (str (:id id1))
        delta  {[::person/id id1] {::person/id    {:after id1}
                                   ::person/email {:after "joe@nowhere.com"}}}]
    (let [{:keys [tempid->string txn]} (datomic/delta->txn *env* :production delta)]
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
    (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/add id ::person/email "joe@nowhere.net"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, native ID, UPDATE attribute"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"}])
        delta {[::person/id id] {::person/full-name {:before "Bob" :after "Bobby"}}}]
    (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/add id ::person/full-name "Bobby"]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, native ID, DELETE attribute"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"}])
        delta {[::person/id id] {::person/full-name {:before "Bob"}}}]
    (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
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
    (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
      (assertions
        "Includes simple add based on real datomic ID"
        txn => [[:db/add id ::person/role :com.fulcrologic.rad.test-schema.person.role/admin]]
        (runnable? txn) => true))))

(specification "delta->txn: simple flat delta, existing entity, native ID, REMOVE to-one enum"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/role      :com.fulcrologic.rad.test-schema.person.role/admin
                                                      ::person/full-name "Bob"}])
        delta {[::person/id id] {::person/role {:before :com.fulcrologic.rad.test-schema.person.role/admin}}}]
    (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
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
    (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
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
    (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
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
  (let [tempid1         (tempid/tempid (ids/new-uuid 1))
        tempid2         (tempid/tempid (ids/new-uuid 2))
        tempid3         (tempid/tempid (ids/new-uuid 3))
        new-address-id1 (ids/new-uuid 1)
        new-address-id2 (ids/new-uuid 2)
        sid1            "ffffffff-ffff-ffff-ffff-000000000001"
        sid2            "ffffffff-ffff-ffff-ffff-000000000002"
        sid3            "ffffffff-ffff-ffff-ffff-000000000003"
        delta           {[::person/id tempid1]  {::person/id              {:after tempid1}
                                                 ::person/full-name       {:after "Tony"}
                                                 ::person/primary-address {:after [::address/id tempid2]}
                                                 ::person/addresses       {:after [[::address/id tempid2] [::address/id tempid3]]}}
                         [::address/id tempid2] {::address/id     {:after tempid2}
                                                 ::address/street "A St"}
                         [::address/id tempid3] {::address/id     {:after tempid3}
                                                 ::address/street "B St"}}]
    (when-mocking
      (datomic/next-uuid) =1x=> new-address-id1
      (datomic/next-uuid) =1x=> new-address-id2

      (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
        (assertions
          "Adds the non-native IDs, and the proper values"
          (set txn) => #{[:db/add sid2 :com.fulcrologic.rad.test-schema.address/id new-address-id1]
                         [:db/add sid3 :com.fulcrologic.rad.test-schema.address/id new-address-id2]
                         [:db/add sid1 :com.fulcrologic.rad.test-schema.person/full-name "Tony"]
                         [:db/add sid1 :com.fulcrologic.rad.test-schema.person/primary-address sid2]
                         [:db/add sid1 :com.fulcrologic.rad.test-schema.person/addresses sid3]
                         [:db/add sid1 :com.fulcrologic.rad.test-schema.person/addresses sid2]}
          (runnable? txn) => true)))))

(specification "Existing entity, add new to-one child"
  (let [{{:strs [id]} :tempids} @(d/transact *conn* [{:db/id             "id"
                                                      ::person/full-name "Bob"
                                                      ::person/addresses [{::address/id     (ids/new-uuid 1)
                                                                           ::address/street "A St"}]}])
        tempid1         (tempid/tempid (ids/new-uuid 1))
        new-address-id1 (ids/new-uuid 1)
        sid1            "ffffffff-ffff-ffff-ffff-000000000001"
        delta           {[::person/id id]       {::person/primary-address {:after [::address/id tempid1]}}
                         [::address/id tempid1] {::address/id     {:after tempid1}
                                                 ::address/street {:after "B St"}}}]
    (when-mocking
      (datomic/next-uuid) =1x=> new-address-id1

      (let [{:keys [txn]} (datomic/delta->txn *env* :production delta)]
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
        tempid1         (tempid/tempid (ids/new-uuid 1))
        new-address-id1 (ids/new-uuid 1)
        sid1            "ffffffff-ffff-ffff-ffff-000000000001"
        delta           {[::person/id id]       {::person/addresses {:before [[::address/id (ids/new-uuid 1)]]
                                                                     :after  [[::address/id (ids/new-uuid 1)] [::address/id tempid1]]}}
                         [::address/id tempid1] {::address/id     {:after tempid1}
                                                 ::address/street {:after "B St"}}}]
    (when-mocking
      (datomic/next-uuid) =1x=> new-address-id1

      (let [{:keys [tempid->string txn]} (datomic/delta->txn *env* :production delta)]
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
        tempid1         (tempid/tempid (ids/new-uuid 100))
        new-address-id1 (ids/new-uuid 100)
        sid1            "ffffffff-ffff-ffff-ffff-000000000100"
        delta           {[::person/id id]       {::person/addresses {:before [[::address/id (ids/new-uuid 1)]]
                                                                     :after  [[::address/id tempid1]]}}
                         [::address/id tempid1] {::address/id     {:after tempid1}
                                                 ::address/street {:after "B St"}}}]
    (when-mocking
      (datomic/next-uuid) =1x=> new-address-id1

      (let [{:keys [tempid->string txn]} (datomic/delta->txn *env* :production delta)]
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

(specification "save-form!" :focus
  (let [_       @(d/transact *conn* [{::address/id (ids/new-uuid 1) ::address/street "A St"}])
        tempid1 (tempid/tempid (ids/new-uuid 100))
        delta   {[::person/id tempid1]           {::person/id              tempid1
                                                  ::person/full-name       {:after "Bob"}
                                                  ::person/primary-address {:after [::address/id (ids/new-uuid 1)]}
                                                  ::person/role            :com.fulcrologic.rad.test-schema.person.role/admin}
                 [::address/id (ids/new-uuid 1)] {::address/street {:before "A St" :after "A1 St"}}}]
    (let [{:keys [tempids]} (datomic/save-form! *env* {::form/delta delta})
          real-id (get tempids tempid1)
          person  (d/pull (d/db *conn*) '[::person/full-name {::person/primary-address [::address/street]}] real-id)]
      (assertions
        "Gives a proper remapping"
        (pos-int? (get tempids tempid1)) => true
        "Updates the db"
        person => {::person/full-name       "Bob"
                   ::person/primary-address {::address/street "A1 St"}}))))
