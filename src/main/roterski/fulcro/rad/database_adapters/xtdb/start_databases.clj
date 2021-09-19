(ns roterski.fulcro.rad.database-adapters.xtdb.start-databases
  (:require
   [xtdb.api :as xt]
   [roterski.fulcro.rad.database-adapters.xtdb-options :as xo]
   [roterski.fulcro.rad.database-adapters.xtdb.wrap-xtdb-save :as wcs]
   [taoensso.timbre :as log]))

(defn transaction-functions->txs [t-fns]
  (->> t-fns
       (reduce (fn [txs [fn-id fn-body]]
                 (conj txs [::xt/put {:xt/id fn-id
                                      :xt/fn fn-body}]))
               [])))

(defn start-database!
  "Starts a xtdb database node given the standard sub-element config described
  in `start-databases`. Typically use that function instead of this one.

  * `:config` a xtdb config map passed directly to xtdb.api/start-node.
  See the documentation https://xtdb.com/reference/1.19.0-beta1/configuration.html

  Returns a migrated database connection."
  [config opts]
  (let [transaction-functions (merge wcs/transaction-functions (xo/transaction-functions opts))
        node (xt/start-node config)]
    (when transaction-functions
      (log/info "Adding transaction functions: " (keys transaction-functions))
      (xt/submit-tx node (transaction-functions->txs transaction-functions))) ;; todo merge with config supplied functions
    (log/info "Schema management disabled.")
    (log/info "Finished connecting to xtdb database.")
    node))

(defn start-databases
  "Start all of the databases described in config, using the schemas defined in schemas.

  * `config`:  a map that contains the key `xo/databases`.

  The `xo/databases` entry in the config is a map with the following form:

  ```
 {:production-shard-1 {:xtdb.jdbc/connection-pool {:dialect #:xtdb{:module \"xtdb.jdbc.psql/->dialect\"}
                                                   :db-spec {:dbname   \"fulcro-rad-demo\"
                                                             :user     \"postgres\"
                                                             :password \"postgres\"}}
                       :xtdb/tx-log               {:xtdb/module \"xtdb.jdbc/->tx-log\"
                                                   :connection-pool :xtdb.jdbc/connection-pool}
                       :xtdb/document-store       {:xtdb/module \"xtdb.jdbc/->document-store\"
                                                   :connection-pool :xtdb.jdbc/connection-pool}}}
  ```
  where the key (i.e. `:production-shard-1`) is a database name and the value is a xtdb config map passed directly to xtdb.api/start-node.
  See the documentation https://xtdb.com/reference/1.19.0-beta1/configuration.html

  NOTE: xtdb expects the value under :xtdb/module key to be a symbol so if you want store the config in edn file, you can
  use strings for :xtdb/module values and pass the config through symbolize-xtdb-modules before calling start-databases:
  ```
  (require '[roterski.fulcro.rad.database-adapters.xtdb :as xtdb])
  (xtdb/start-databases (xtdb/symbolize-xtdb-modules config))
  ```

  * `options`: a map that contains xo/transaction-functions
 
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
    (xo/databases config))))
