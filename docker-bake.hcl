variable "TARGET" {
  default = "$TARGET"
}
variable "DOCKER_PLATFORM" {
  default = "$DOCKER_PLATFORM"
}
target "docker-metadata-action" {}
target "build" {
  context = "./"
  dockerfile = "Dockerfile"
  target = "binaries"
  platforms = [
    "${DOCKER_PLATFORM}"
  ]
  cache-from = [
    "type=gha,scope=${TARGET}"
  ]
  cache-to = [
    "type=gha,scope=${TARGET},mode=max"
  ]
  output = ["./artifact"]
}
target "image" {
  inherits = ["build", "docker-metadata-action"]
  target = "image"
  cache-to = [""]
  output = [""]
}
