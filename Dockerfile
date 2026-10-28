# Étape 1 : Build du frontend (React + Tailwind)
FROM node:22-alpine AS builder-node
WORKDIR /app/web
COPY web/package.json ./
RUN npm install
COPY web/ ./
RUN npm run build

# Étape 2 : Build du backend (Go)
FROM golang:1.22-alpine AS builder-go
WORKDIR /app
COPY go.mod ./
COPY main.go ./
# On récupère le build React de l'étape précédente pour le //go:embed
COPY --from=builder-node /app/web/dist ./web/dist
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o alert-console main.go

# Étape 3 : Image finale ultra-légère d'exécution
FROM alpine:latest
WORKDIR /app
RUN apk add --no-cache tzdata
COPY --from=builder-go /app/alert-console .

EXPOSE 8080
CMD ["./alert-console"]
