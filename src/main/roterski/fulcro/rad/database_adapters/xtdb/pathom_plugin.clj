(ns roterski.fulcro.rad.database-adapters.xtdb.pathom-plugin
  (:require
   [xtdb.api :as xt]
   [roterski.fulcro.rad.database-adapters.xtdb-options :as xo]
   [taoensso.encore :as enc]))

(defn wrap-env
  ([database-adapter]
   (wrap-env nil database-adapter))
  ([base-wrapper database-mapper]
   (fn [env]
     (let [database-node-map (database-mapper env)
           databases         (enc/map-vals (fn [node] (atom (xt/db node))) database-node-map)]
       (cond-> (assoc env
                      xo/nodes database-node-map
                      xo/databases databases)
         base-wrapper (base-wrapper))))))

(defn pathom-plugin
  "A pathom 2 plugin that adds the necessary xtdb nodes and databases to the pathom env for
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
  (let [augment (wrap-env database-mapper)]
    {:com.wsscode.pathom.core/wrap-parser
     (fn env-wrap-wrap-parser [parser]
       (fn env-wrap-wrap-internal [env tx]
         (parser (augment env) tx)))}))
