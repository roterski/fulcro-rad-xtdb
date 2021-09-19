(ns roterski.fulcro.rad.database-adapters.xtdb-options)

;; Attribute options

;; Other

(def nodes
  "If using the xtdb pathom-plugin, the resulting pathom-env will contain
    a map from schema->node at this key path"
  :roterski.fulcro.rad.database-adapters.xtdb/nodes)

(def databases
  "If using the xtdb pathom-plugin, the resulting pathom-env will contain
    a map from schema->database at this key path"
  :roterski.fulcro.rad.database-adapters.xtdb/databases)

(def transaction-functions
  "A way to a add custom transaction functions to each xtdb node.
   Should be a map where the key is a name of the function and the value (that needs to be a symbol) is a body of the function
   See https://xtdb.com/reference/1.19.0-beta1/transactions.html#transaction-functions
   ```
   {::example-fn '(fn [ctx [id {:keys [before after]}]]
                     (let [db (xtdb.api/db ctx)
                           entity (xtdb.api/entity db id)]
                       [[:xtdb.api/match id (some-> entity (merge before))]
                        [:xtdb.api/put (merge entity after)]]))}
   ```"
  :roterski.fulcro.rad.database-adapters.xtdb/transaction-functions)
