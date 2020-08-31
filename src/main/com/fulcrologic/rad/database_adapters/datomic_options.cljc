(ns com.fulcrologic.rad.database-adapters.datomic-options)

;; Attribute options

(def native-id?
  "If true it will map the given ID attribute (which must be type long) to :db/id."
  :com.fulcrologic.rad.database-adapters.datomic/native-id?)

(def attribute-schema
  "a map of datomic schema attributes to be included in transacted schema.
    example:  {:db/isComponent true}
  "
  :com.fulcrologic.rad.database-adapters.datomic/attribute-schema)

;; Other

(def connections
  "If using the datomic pathom-plugin, the resulting pathm-env will contain
    a map from schema->connection at this key path"
  :com.fulcrologic.rad.database-adapters.datomic/connections)

(def databases
  "If using the datomic pathom-plugin, the resulting pathm-env will contain
    a map from schema->database at this key path"
  :com.fulcrologic.rad.database-adapters.datomic/databases)

