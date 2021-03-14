(ns com.fulcrologic.rad.database-adapters.crux.pathom-plugin
  (:require
   [crux.api :as c]
   [com.fulcrologic.rad.database-adapters.crux-options :as co]
   [com.rpl.specter :as sp]
   [com.wsscode.pathom.core :as p]))

(defn pathom-plugin
  "A pathom plugin that adds the necessary Datomic connections and databases to the pathom env for
  a given request. Requires a database-mapper, which is a
  `(fn [pathom-env] {schema-name connection})` for a given request.

  The resulting pathom-env available to all resolvers will then have:

  - `co/nodes`: The result of database-mapper
  - `co/databases`: A map from schema name to atoms holding a database. The atom is present so that
  a mutation that modifies the database can choose to update the snapshot of the db being used for the remaining
  resolvers.

  This plugin should run before (be listed after) most other plugins in the plugin chain since
  it adds connection details to the parsing env.
  "
  [database-mapper]
  (p/env-wrap-plugin
   (fn [env]
     (let [database-node-map (database-mapper env)
           databases               (sp/transform [sp/MAP-VALS] (fn [v] (atom (c/db v))) database-node-map)]
       (assoc env
              co/nodes database-node-map
              co/databases databases)))))
