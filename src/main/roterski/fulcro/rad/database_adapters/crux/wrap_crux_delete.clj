(ns roterski.fulcro.rad.database-adapters.crux.wrap-crux-delete
  (:require
   [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
   [com.fulcrologic.rad.attributes :as attr]
   [roterski.fulcro.rad.database-adapters.crux-options :as co]
   [com.fulcrologic.rad.form :as form]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]
   [crux.api :as c]))

(defn delete-entity!
  "Delete the given entity, if possible."
  [{::attr/keys [key->attribute] :as env} params]
  (enc/if-let [pk         (ffirst params)
               id         (get params pk)
               ident      [pk id]
               {:keys [::attr/schema]} (key->attribute pk)
               node (-> env co/nodes (get schema))]
    (do
      (log/info "Deleting" ident)
      (let [database-atom (get-in env [co/databases schema])
            tx (c/submit-tx node [[:crux.tx/delete id]])]
        (c/await-tx node tx)
        (when database-atom
          (reset! database-atom (c/db node)))
        {}))
    (log/warn "Crux adapter failed to delete " params)))

(defn wrap-crux-delete
  "Form delete middleware to accomplish crux deletes."
  ([handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [local-result   (delete-entity! pathom-env params)
           handler-result (handler pathom-env)]
       (deep-merge handler-result local-result))))
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (delete-entity! pathom-env params))))

