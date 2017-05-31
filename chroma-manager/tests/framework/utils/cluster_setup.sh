# Remove test results and coverage reports from previous run
rm -rfv $PWD/test_reports/*
rm -rfv $PWD/coverage_reports/.coverage*
mkdir -p $PWD/test_reports
mkdir -p $PWD/coverage_reports

CLUSTER_CONFIG=${CLUSTER_CONFIG:-"$(ls $PWD/shared_storage_configuration_cluster_cfg.json)"}
CHROMA_DIR=${CHROMA_DIR:-"$PWD/intel-manager-for-lustre/"}
USE_FENCE_XVM=false

eval $(python $CHROMA_DIR/chroma-manager/tests/utils/json_cfg2sh.py "$CLUSTER_CONFIG")

MEASURE_COVERAGE=${MEASURE_COVERAGE:-true}
PROXY=${PROXY:-''} # Pass in a command that will set your proxy settings iff the cluster is behind a proxy. Ex: PROXY="http_proxy=foo https_proxy=foo"

echo "Beginning installation and setup..."

# put some keys on the nodes for easy access by developers
# and make sure EPEL is enabled
pdsh -l root -R ssh -S -w $(spacelist_to_commalist $ALL_NODES) "exec 2>&1; set -xe
cat <<\"EOF\" >> /root/.ssh/authorized_keys
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCrcI6x6Fv2nzJwXP5mtItOcIDVsiD0Y//LgzclhRPOT9PQ/jwhQJgrggPhYr5uIMgJ7szKTLDCNtPIXiBEkFiCf9jtGP9I6wat83r8g7tRCk7NVcMm0e0lWbidqpdqKdur9cTGSOSRMp7x4z8XB8tqs0lk3hWefQROkpojzSZE7fo/IT3WFQteMOj2yxiVZYFKJ5DvvjdN8M2Iw8UrFBUJuXv5CQ3xV66ZvIcYkth3keFk5ZjfsnDLS3N1lh1Noj8XbZFdSRC++nbWl1HfNitMRm/EBkRGVP3miWgVNfgyyaT9lzHbR8XA7td/fdE5XrTpc7Mu38PE7uuXyLcR4F7l brian@brian-laptop
EOF
# instruct any caching proxies to only cache packages
ed /etc/yum.conf <<EOF
/^$/i
http_caching=packages
.
wq
EOF
rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
yum-config-manager --enable addon-epel\$(rpm --eval %rhel)-x86_64
yum-config-manager --add-repo https://copr-be.cloud.fedoraproject.org/results/managerforlustre/manager-for-lustre/epel-7-x86_64/
yum-config-manager --add-repo http://mirror.centos.org/centos/7/extras/x86_64/
if [[ \$HOSTNAME = *vm*9 ]]; then
    build_type=client
else
    build_type=server
fi
yum-config-manager --add-repo https://build.whamcloud.com/job/lustre-master/lastSuccessfulBuild/arch=x86_64,build_type=\$build_type,distro=el7,ib_stack=inkernel/artifact/artifacts/
sed -i -e '1d' -e '2s/^.*$/[lustre]/' -e '/baseurl/s/,/%2C/g' -e '/enabled/a gpgcheck=0' /etc/yum.repos.d/build.whamcloud.com_job_lustre-master_lastSuccessfulBuild_arch\=x86_64\,build_type\=\$build_type\,distro\=el7\,ib_stack\=inkernel_artifact_artifacts_.repo
yum-config-manager --add-repo https://build.whamcloud.com/job/e2fsprogs-master/arch=x86_64,distro=el7/lastSuccessfulBuild/artifact/_topdir/RPMS/
sed -i -e '1d' -e '2s/^.*$/[e2fsprogs]/' -e '/baseurl/s/,/%2C/g' -e '/enabled/a gpgcheck=0' /etc/yum.repos.d/build.whamcloud.com_job_e2fsprogs-master_arch\=x86_64\,distro\=el7_lastSuccessfulBuild_artifact__topdir_RPMS_.repo
yum -y install distribution-gpg-keys-copr
if ! ls /usr/share/distribution-gpg-keys/copr/copr-*manager-for-lustre*; then
    rpm --import https://copr-be.cloud.fedoraproject.org/results/managerforlustre/manager-for-lustre/pubkey.gpg
fi" | dshbak -c
if [ ${PIPESTATUS[0]} != 0 ]; then
    exit 1
fi
