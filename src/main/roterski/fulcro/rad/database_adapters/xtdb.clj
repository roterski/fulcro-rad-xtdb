(ns roterski.fulcro.rad.database-adapters.xtdb
  (:require
   [roterski.fulcro.rad.database-adapters.xtdb.start-databases :as sd]
   [roterski.fulcro.rad.database-adapters.xtdb.pathom-plugin :as pp]
   [roterski.fulcro.rad.database-adapters.xtdb.generate-resolvers :as gr]
   [roterski.fulcro.rad.database-adapters.xtdb.wrap-xtdb-save :as wcs]
   [roterski.fulcro.rad.database-adapters.xtdb.wrap-xtdb-delete :as wcd]
   [clojure.walk :as walk]))

(defn symbolize-xtdb-modules [config]
  (walk/postwalk
   #(cond-> %
      (and (map? %) (contains? % :xtdb/module)) (update :xtdb/module symbol))
   config))

(def start-databases sd/start-databases)

(def pathom-plugin pp/pathom-plugin)

(def generate-resolvers gr/generate-resolvers)

(def wrap-xtdb-save wcs/wrap-xtdb-save)

(def wrap-xtdb-delete wcd/wrap-xtdb-delete)
