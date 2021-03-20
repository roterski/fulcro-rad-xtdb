(ns com.fulcrologic.rad.database-adapters.crux
  (:require
   [com.fulcrologic.rad.database-adapters.crux.start-databases :as sd]
   [com.fulcrologic.rad.database-adapters.crux.pathom-plugin :as pp]
   [com.fulcrologic.rad.database-adapters.crux.generate-resolvers :as gr]
   [com.fulcrologic.rad.database-adapters.crux.wrap-crux-save :as wcs]
   [com.fulcrologic.rad.database-adapters.crux.wrap-crux-delete :as wcd]
   [clojure.walk :as walk]))

(defn symbolize-crux-modules [config]
  (walk/postwalk
   #(cond-> %
      (and (map? %) (contains? % :crux/module)) (update :crux/module symbol))
   config))

(def start-databases sd/start-databases)

(def pathom-plugin pp/pathom-plugin)

(def generate-resolvers gr/generate-resolvers)

(def wrap-crux-save wcs/wrap-crux-save)

(def wrap-crux-delete wcd/wrap-crux-delete)
