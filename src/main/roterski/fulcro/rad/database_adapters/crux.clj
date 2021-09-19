(ns roterski.fulcro.rad.database-adapters.crux
  (:require
   [roterski.fulcro.rad.database-adapters.crux.start-databases :as sd]
   [roterski.fulcro.rad.database-adapters.crux.pathom-plugin :as pp]
   [roterski.fulcro.rad.database-adapters.crux.generate-resolvers :as gr]
   [roterski.fulcro.rad.database-adapters.crux.wrap-crux-save :as wcs]
   [roterski.fulcro.rad.database-adapters.crux.wrap-crux-delete :as wcd]
   [clojure.walk :as walk]))

(defn symbolize-xtdb-modules [config]
  (walk/postwalk
   #(cond-> %
      (and (map? %) (contains? % :xtdb/module)) (update :xtdb/module symbol))
   config))

(def start-databases sd/start-databases)

(def pathom-plugin pp/pathom-plugin)

(def generate-resolvers gr/generate-resolvers)

(def wrap-crux-save wcs/wrap-crux-save)

(def wrap-crux-delete wcd/wrap-crux-delete)
