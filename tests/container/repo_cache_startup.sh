#!/bin/bash
set -euo pipefail
image="${1:?Pass the backend image to test}"
volumes=()
container=''
base=$(mktemp -d)
cleanup() {
  if [ -n "$container" ]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi
  if [ ${#volumes[@]} -gt 0 ]; then docker volume rm "${volumes[@]}" >/dev/null; fi
  docker run --rm --user 0:0 -v "$base:/fixture" "$image" chown -R -P --no-dereference "$(id -u):$(id -g)" /fixture
  rm -rf -- "$base"
}
trap cleanup EXIT
for scenario in volume bind custom; do
 path=repo_cache
 [ "$scenario" != custom ] || path='/app/custom cache'
 dest="$path"
 [[ "$dest" = /* ]] || dest="/app/$dest"
 if [ "$scenario" = bind ]; then
  mkdir "$base/cache"
  mount="type=bind,src=$base/cache,dst=$dest"
 else
  vol=$(docker volume create); volumes+=("$vol"); mount="type=volume,src=$vol,dst=$dest"
 fi
 common=(--network none --mount "$mount" -e "PU_SAVE_REPO_PATH=$path")
 docker run --rm "${common[@]}" --user root "$image" bash -euc '
  c="$PU_SAVE_REPO_PATH"; chown root:root "$c"; chmod 700 "$c"; git init -q "$c/repo"; git -C "$c/repo" config user.name Test; git -C "$c/repo" config user.email test@example.invalid; echo original > "$c/repo/data"; git -C "$c/repo" add data; git -C "$c/repo" commit -qm seed; mkdir "$c/repo/private"; echo preserved > "$c/repo/private/data"; chmod 700 "$c/repo/private"; chmod 600 "$c/repo/private/data"; chown 12345:12345 "$c/repo/private/data"; ln -s /app/README.md "$c/repo/outside"'
 for pass in 1 2; do
  container=$(docker run -d "${common[@]}" -e PU_SQLALCHEMY_DATABASE_URL=postgresql://u:p@127.0.0.1:5432/x "$image")
  ready=0
  for i in $(seq 1 50); do
   if docker logs "$container" 2>&1 | grep -q 'Wait Ready PostgreSQL'; then ready=1; break; fi
   if [ "$(docker inspect -f '{{.State.Running}}' "$container")" != true ]; then docker logs "$container"; exit 1; fi
   sleep .2
  done
  [ "$ready" = 1 ] || { docker logs "$container"; exit 1; }
  docker exec "$container" bash -ec 'grep -Eq "^Uid:[[:space:]]+10001[[:space:]]+10001" /proc/1/status; grep -Eq "^CapEff:[[:space:]]+0+$" /proc/1/status; grep -Eq "^CapBnd:[[:space:]]+0+$" /proc/1/status; grep -Eq "^NoNewPrivs:[[:space:]]+1" /proc/1/status; test "$(stat -c %u /app/README.md)" = 0; test -L "$PU_SAVE_REPO_PATH/repo/outside"; test "$(cat "$PU_SAVE_REPO_PATH/repo/private/data")" = preserved; test -z "$(find "$PU_SAVE_REPO_PATH" \( ! -uid 10001 -o ! -gid 10001 \) -print -quit)"'
  docker exec --user 10001:10001 "$container" bash -ec 'git -C "$PU_SAVE_REPO_PATH/repo" status --porcelain >/dev/null; echo more >> "$PU_SAVE_REPO_PATH/repo/data"; git -C "$PU_SAVE_REPO_PATH/repo" add data; git -C "$PU_SAVE_REPO_PATH/repo" commit -qm more; git clone -q "$PU_SAVE_REPO_PATH/repo" /tmp/clone-test; git -C /tmp/clone-test fetch -q origin'
  docker rm -f "$container" >/dev/null; container=''
 done
 echo "PASS $scenario: root-owned cache, startup drops privileges, repeated start and Git"
done
