./mvnw versions:set -DnewVersion="0.13.1-incubating-SNAPSHOT-au-$FINAL_OR_BRANCH_SNAPSHOT_VERSION"
./mvnw -DskipTests=true -Pdist install

sed "s%<DRUID_VERSION>%0.13.1-incubating-SNAPSHOT-au-$FINAL_OR_BRANCH_SNAPSHOT_VERSION%g" Dockerfile.template > Dockerfile
