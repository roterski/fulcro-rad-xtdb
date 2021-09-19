(ns roterski.fulcro.rad.database-adapters.xtdb.pathom-plugin
  (:require
   [xtdb.api :as xt]
   [roterski.fulcro.rad.database-adapters.xtdb-options :as xo]
   [com.rpl.specter :as sp]
   [com.wsscode.pathom.core :as p]))

(defn pathom-plugin
  "A pathom plugin that adds the necessary xtdb nodes and databases to the pathom env for
  a given request. Requires a database-mapper, which is a
  `(fn [pathom-env] {schema-name connection})` for a given request.

  The resulting pathom-env available to all resolvers will then have:

  - `xo/nodes`: The result of database-mapper
  - `xo/databases`: A map from schema name to atoms holding a database. The atom is present so that
  a mutation that modifies the database can choose to update the snapshot of the db being used for the remaining
  resolvers.

  This plugin should run before (be listed after) most other plugins in the plugin chain since
  it adds connection details to the parsing env.
  "
  [database-mapper]
  (p/env-wrap-plugin
   (fn [env]
     (let [database-node-map (database-mapper env)
           databases         (sp/transform [sp/MAP-VALS] (fn [v] (atom (xt/db v))) database-node-map)]
       (assoc env
              xo/nodes database-node-map
              xo/databases databases)))))
