(ns roterski.fulcro.rad.database-adapters.xtdb.wrap-xtdb-delete
  (:require
   [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
   [com.fulcrologic.rad.attributes :as attr]
   [roterski.fulcro.rad.database-adapters.xtdb-options :as xo]
   [com.fulcrologic.rad.form :as form]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]
   [xtdb.api :as xt]))

(defn delete-entity!
  "Delete the given entity, if possible."
  [{::attr/keys [key->attribute] :as env} params]
  (enc/if-let [pk         (ffirst params)
               id         (get params pk)
               ident      [pk id]
               {:keys [::attr/schema]} (key->attribute pk)
               node (-> env xo/nodes (get schema))]
    (do
      (log/info "Deleting" ident)
      (let [database-atom (get-in env [xo/databases schema])
            tx (xt/submit-tx node [[::xt/delete id]])]
        (xt/await-tx node tx)
        (when database-atom
          (reset! database-atom (xt/db node)))
        {}))
    (log/warn "xtdb adapter failed to delete " params)))

(defn wrap-xtdb-delete
  "Form delete middleware to accomplish xtdb deletes."
  ([handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [local-result   (delete-entity! pathom-env params)
           handler-result (handler pathom-env)]
       (deep-merge handler-result local-result))))
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (delete-entity! pathom-env params))))

