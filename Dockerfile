FROM golang:1.22.5

# Set destination for COPY
WORKDIR /etl

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code. Note the slash at the end, as explained in
# https://docs.docker.com/engine/reference/builder/#copy
#COPY app ./app

# Build
#RUN CGO_ENABLED=0 GOOS=linux go build -o etl ./app

#EXPOSE 8080

# Run
CMD ["./etl"]