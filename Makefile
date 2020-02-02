- run-backend:
	@go run ./cmd/project-card-backend/main.go

- build:
	@go build -o ./build/project-card-backend ./cmd/project-card-backend/main.go
	@./build/project-card-backend

- docker-run:
	@docker build -t backend .
	@docker run --name backend -d -p 8081:8080 --env-file=./.env backend
