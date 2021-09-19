(ns roterski.fulcro.rad.database-adapters.crux.start-databases
  (:require
   [xtdb.api :as xt]
   [roterski.fulcro.rad.database-adapters.crux-options :as co]
   [roterski.fulcro.rad.database-adapters.crux.wrap-crux-save :as wcs]
   [taoensso.timbre :as log]))

(defn transaction-functions->txs [t-fns]
  (->> t-fns
       (reduce (fn [txs [fn-id fn-body]]
                 (conj txs [::xt/put {:xt/id fn-id
                                      :xt/fn fn-body}]))
               [])))

(defn start-database!
  "Starts a Crux database node given the standard sub-element config described
  in `start-databases`. Typically use that function instead of this one.

  * `:config` a crux config map passed directly to xtdb.api/start-node.
  See the documentation https://opencrux.com/reference/21.02-1.15.0/configuration.html

  Returns a migrated database connection."
  [config opts]
  (let [transaction-functions (merge wcs/transaction-functions (co/transaction-functions opts))
        node (xt/start-node config)]
    (when transaction-functions
      (log/info "Adding transaction functions: " (keys transaction-functions))
      (xt/submit-tx node (transaction-functions->txs transaction-functions))) ;; todo merge with config supplied functions
    (log/info "Schema management disabled.")
    (log/info "Finished connecting to crux database.")
    node))

(defn start-databases
  "Start all of the databases described in config, using the schemas defined in schemas.

  * `config`:  a map that contains the key `co/databases`.

  The `co/databases` entry in the config is a map with the following form:

  ```
 {:production-shard-1 {:crux.jdbc/connection-pool {:dialect #:crux{:module \"crux.jdbc.psql/->dialect\"}
                                                   :db-spec {:dbname   \"fulcro-rad-demo\"
                                                             :user     \"postgres\"
                                                             :password \"postgres\"}}
                       :xtdb/tx-log               {:xtdb/module \"crux.jdbc/->tx-log\"
                                                   :connection-pool :crux.jdbc/connection-pool}
                       :xtdb/document-store       {:xtdb/module \"crux.jdbc/->document-store\"
                                                   :connection-pool :crux.jdbc/connection-pool}}}
  ```
  where the key (i.e. `:production-shard-1`) is a database name and the value is a crux config map passed directly to xtdb.api/start-node.
  See the documentation https://opencrux.com/reference/21.02-1.15.0/configuration.html

  NOTE: crux expects the value under :xtdb/module key to be a symbol so if you want store the config in edn file, you can
  use strings for :xtdb/module values and pass the config through symbolize-xtdb-modules before calling start-databases:
  ```
  (require '[roterski.fulcro.rad.database-adapters.crux :as crux])
  (crux/start-databases (crux/symbolize-xtdb-modules config))
  ```

  * `options`: a map that contains co/transaction-functions
 
  Each supported driver type has custom options for configuring it. See Fulcro's config
  file support for a good method of defining these in EDN config files for use in development
  and production environments.

  Returns a map whose keys are the database keys (i.e. `:production-shard-1`) and
  whose values are the live database connection.
  "
  ([config]
   (start-databases config {}))
  ([config options]
   (reduce-kv
    (fn [m k v]
      (log/info "Starting database " k)
      (assoc m k (start-database! v options)))
    {}
    (co/databases config))))
