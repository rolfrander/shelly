(ns shelly.main 
    (:require [clojurewerkz.machine-head.client :as mqtt]
              [clojure.data.json :as json]
              [clj-yaml.core :as yaml]
              [clojure.core.async :as a :refer [<!! >!! <! >!]]
              [clojure.string :as str]
              ))


(let [curr-id (atom 0)
      callbacks (atom {})
      conn (mqtt/connect "tcp://192.168.2.9:1883"
                         {:client-id "config"
                          :opts {:username "test" :password "test"}})]

  (defn callbacks [] @callbacks)

  (defn close []
    (mqtt/disconnect conn))
  
  (defn listen []
    (mqtt/subscribe conn
                    {"hellerud/config/rpc" 0}
                    (fn [^String topic metadata ^bytes payload]
                      ;(println (String. payload "UTF-8"))
                      (try (let [data (json/read-str (String. payload "UTF-8")
                                                     :key-fn keyword)
                                 id (:id data)
                                 callback (get @callbacks id)]
                             (swap! callbacks dissoc id)
                             (condp #(contains? %2 %1) data
                               :result ((:ok callback) (:result data))
                               :error ((:error callback) (:code (:error data)) (:message (:error data)))))
                           (catch Exception e (println "Error: " (.getMessage e)))))))

  (defn call [dst method params callback-ok callback-error]
    (let [id (swap! curr-id inc)]
      (swap! callbacks assoc id {:ok (or callback-ok (constantly nil))
                                 :error (or callback-error (constantly nil))})
      (mqtt/publish conn
                    (str "hellerud/iot/" dst "/rpc")
                    (json/json-str {:id id
                                    :src "hellerud/config"
                                    :method method
                                    :params params}
                                   :escape-slash false)))))

(defn call-wait [dst method params]
  (let [ret (a/chan)]
    (call dst method params 
          (fn [x] (>!! ret [:ok x]))
          (fn [x y] (>!! ret [:err [x y]])))
    (let [[status val] (<!! ret)]
      (a/close! ret)
      (if (= :ok status) 
        val
        (throw (RuntimeException. (str val)))))))

(listen)

(callbacks)

(close)

(call-wait "stikk01" "Webhook.List" {})

(call "stikk01" "Webhook.DeleteAll" {} nil nil)

(defn index-map [in k]
  (zipmap (map k in) in))

(defn read-hosts [hosts-file]
  (->> (slurp hosts-file)
       (str/split-lines)
       (map #(str/split % #" +"))
       (into {})))



(defn resolve-url [url]
  (let [hosts (read-hosts "/home/rolfn/automation/shelly/hosts.txt")
        parsed-url (clojure.java.io/as-url url)
        host (.getHost parsed-url)
        ip (hosts host)]
    (if ip (str (java.net.URL. (.getProtocol parsed-url)
                               ip
                               (.getPort parsed-url)
                               (.getFile parsed-url)))
        url)))

(defn update-webhooks [webhooks-file]
  (let [webhooks-cfg (yaml/parse-string (slurp webhooks-file))
        handle-response (fn [data])]
    (when (contains? webhooks-cfg :webhooks)
      (let [existing (-> (get (call-wait "stikk01" "Webhook.List" {}) :hooks)
                         (index-map :name))]
        (doseq [wh (:webhooks webhooks-cfg)]
          (let [name (:name wh)
                wh (update wh :url (partial map resolve-url))]
            (if (or (nil? name)
                    (not (contains? existing name)))
              (call "stikk01" "Webhook.Create" wh nil println)
              (call "stikk01" "Webhook.Update" (assoc wh :id (:id (get existing name))) nil println))))))))


(update-webhooks "/home/rolfn/automation/shelly/stikk01/webhooks.yaml")