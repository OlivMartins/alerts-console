# Elastic Alert Console

> Une console web SRE **100 % auto-suffisante** conçue pour gérer et unifier les alertes Elasticsearch (générées via les **Règles Kibana** et les **Watchers natifs**), sans dépendre de l'API HTTP lourde de Kibana.

---

## 🏗️ Architecture & Philosophie SRE

L'application repose sur le pattern **Single-Binary BFF (Backend-For-Frontend)** :
* **Backend (Go / Golang) :** Un binaire unique ultra-léger (< 15 Mo) qui embarque le build statique de l'interface via `//go:embed`. Il interroge directement les index système d'Elasticsearch (`.watches`, `.watcher-history-*`, `.kibana*`, `.internal.alerts-*`) via un unique appel `_msearch` toutes les 10 secondes.
* **Diffusion Temps Réel (SSE) :** Le backend maintient un flux **Server-Sent Events** pour pousser les mises à jour d'état vers le navigateur sans la complexité d'un WebSocket.
* **Frontend (React + Vite + Tailwind CSS) :** Une interface moderne, sombre (Dark Mode natif), réactive, dotée d'une **vue topologique interactive** pour corréler instantanément une alerte avec son composant d'infrastructure impacté (`OTel` ➔ `Kafka` ➔ `Vector` ➔ `ClickHouse`).

---

## 🗺️ Schéma Topologique & Impact Architecture

La console intègre une vue topologique du pipeline de télémétrie :
[ OTel Collectors ] ──(OTLP)──> [ Kafka Cluster ] ──(Consumer)──> [ Vector Pipeline ] ──(Batch Sink)──> [ ClickHouse / 0ES ]
* **Surlignage en rouge vif :** Dès qu'une alerte est déclenchée (P1 / P2), la brique correspondante s'illumine et pulse en rouge.
* **Corrélation croisée :** Cliquer sur une alerte dans le tableau sélectionne et met en valeur la brique impactée dans le schéma d'architecture.

---

## 🚀 Démarrage Rapide (Via Docker Compose)

Aucun prérequis (ni Go, ni Node.js) n'est nécessaire sur votre machine grâce au conteneur multi-stage Docker.

### 1. Cloner le dépôt
```bash
git clone [https://github.com/OlivMartins/alerts-console.git](https://github.com/OlivMartins/alerts-console.git)
cd alerts-console

2. Configurer le docker-compose.yml et lancer

Par défaut, l'application démarre en Mode Démo SRE interactif (avec un jeu complet de 10 fausses alertes pour tester immédiatement l'interface).

docker-compose.yml

services:
  alert-console:
    build: .
    container_name: es-alert-console
    ports:
      - "8080:8080"
    environment:
      # true = Mode démo avec fausses données | false = Connexion à un cluster réel
      - DEMO_MODE=true
      # URL et Clé API de votre cluster Elasticsearch (si DEMO_MODE=false)
      # - ES_URL=https://mon-cluster-es:9200
      # - ES_API_KEY=VOTRE_CLE_API_BASE64
      # - ES_INSECURE_TLS=true
    restart: unless-stopped

Puis

docker compose up --build -d

Ouvrez ensuite votre navigateur sur http://localhost:8080
