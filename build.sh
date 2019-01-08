./mvnw versions:set -DnewVersion="au-0.13.0-incubating-$FINAL_OR_BRANCH_SNAPSHOT_VERSION"
./mvnw -DskipTests=true -Pdist deploy

sed "s%<DRUID_VERSION>%au-0.13.0-incubating-$FINAL_OR_BRANCH_SNAPSHOT_VERSION%g" Dockerfile.template > Dockerfile
