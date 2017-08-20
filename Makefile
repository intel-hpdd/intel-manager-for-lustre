BUILDER_IS_EL = $(shell rpm --eval '%{?rhel:true}%{!?rhel:false}')

# Top-level Makefile
SUBDIRS ?= $(shell find . -mindepth 2 -maxdepth 2 -name Makefile | sed  -e '/.*\.old/d' -e 's/^\.\/\([^/]*\)\/.*$$/\1/')

.PHONY: all rpms docs subdirs $(SUBDIRS) tags

all: TARGET=all
rpms: TARGET=rpms
docs: TARGET=docs
download: TARGET=download

all rpms docs download subdirs: $(SUBDIRS)

cleandist:
	rm -rf dist

dist: cleandist
	mkdir dist

agent:
	# On non-EL[67] builders, we'll only do an agent build
	$(BUILDER_IS_EL) || $(MAKE) -C chroma-agent $(TARGET)
	$(BUILDER_IS_EL) || cp -a chroma-agent/dist/* dist/

$(SUBDIRS): dist agent
	set -e; \
	if $(BUILDER_IS_EL); then \
		$(MAKE) -C $@ $(TARGET); \
		if [ $(TARGET) != download -a -d $@/dist/ ]; then \
			cp -a $@/dist/* dist/; \
		fi; \
	fi

repo: rpms
	$(MAKE) -C chroma-dependencies repo

bundles: repo
	$(MAKE) -C chroma-bundles

deps: repo

tags:
	#find chroma-agent/chroma_agent chroma-manager/{tests,chroma_{agent_comms,api,cli,core,ui}} -type f | ctags -L -
	ctags --python-kinds=-i -R --exclude=chroma-manager/_topdir --exclude=chroma-\*/myenv\* --exclude=chroma-manager/chroma_test_env --exclude=chroma-dependencies --exclude=chroma_unit_test_env --exclude=chroma-manager/ui-modules .

# build the chroma-{agent,management} subdirs before the chroma-dependencies subdir
chroma-dependencies: chroma-agent chroma-manager chroma-diagnostics
chroma-bundles: chroma-dependencies

destroy_cluster: Vagrantfile
	vagrant destroy
	sed -ie '/# VAGRANT START/,/# VAGRANT END/d' ~/.ssh/config
	sed -ie '/IML Vagrant cluster/d' ~/.ssh/authorized_keys
	export LIBVIRT_DEFAULT_URI=qemu:///system;                       \
	for net in intel-manager-for-lustre{0,1,2,3} vagrant-libvirt; do \
	    virsh net-destroy $$net || true;                             \
	    virsh net-undefine $$net || true;                            \
	done

create_cluster:
	vagrant up
	(echo "# VAGRANT START"; vagrant ssh-config; echo "# VAGRANT END") >> ~/.ssh/config
	# need to have the ssh key that the VMs will use to reach back
	# for virsh commands in .ssh/authorized_keys
	set -e;                                               \
	if ! grep -qf id_rsa.pub ~/.ssh/authorized_keys; then \
	    (echo -n "command=\"$$PWD/vagrant-virsh\" ";      \
	     cat id_rsa.pub) >> ~/.ssh/authorized_keys;       \
	fi
	set -e;                                                                   \
	export LIBVIRT_DEFAULT_URI=qemu:///system;                                \
	if ! virsh list --all | grep -q intel-manager-for-lustre_vm; then         \
	    exit 0;                                                               \
	fi;                                                                       \
	EDITOR=./edit_network virsh net-edit vagrant-libvirt;                     \
	virsh net-destroy vagrant-libvirt;                                        \
	virsh net-start vagrant-libvirt;                                          \
	stopped_nodes="";                                                         \
	for node in {2..9}; do                                                    \
	    stopped_nodes+="$$node";                                              \
	    virsh shutdown intel-manager-for-lustre_vm$$node;                     \
	done;                                                                     \
	for node in {5..8}; do                                                    \
	    if !  virsh dumpxml intel-manager-for-lustre_vm$$node |               \
	      grep "<controller type='scsi' index='0' model='virtio-scsi'>"; then \
	        EDITOR=./edit_scsi virsh edit intel-manager-for-lustre_vm$$node;  \
	        echo "Modified $$vm to use virtio-scsi";                          \
	    else                                                                  \
	        echo "Interesting.  $$vm already has virtio-scsi support in it";  \
	    fi;                                                                   \
	done;                                                                     \
	started_nodes="";                                                         \
	while [ -n "$$stopped_nodes" ]; do                                        \
	    for node in {2..9}; do                                                \
	        if [[ $$stopped_nodes = *$$node* ]] &&                            \
	          ! virsh list | grep -q intel-manager-for-lustre_vm$$node; then  \
	            virsh start intel-manager-for-lustre_vm$$node;                \
	            stopped_nodes=$${stopped_nodes/$$node/};                      \
	            started_nodes+="$$node";                                      \
	        fi;                                                               \
	    done;                                                                 \
	    sleep 1;                                                              \
	done;                                                                     \
	while [ -n "$$started_nodes" ]; do                                        \
	    for node in {2..9}; do                                                \
	        if [[ $$started_nodes = *$$node* ]] &&                            \
	          ssh vm$$node hostname; then                                     \
	            started_nodes=$${started_nodes/$$node/};                      \
	        fi;                                                               \
	    done;                                                                 \
	    sleep 1;                                                              \
	done

reset_cluster: destroy_cluster create_cluster

install_production: reset_cluster
	bash -x scripts/install_dev_cluster

# To run a specific test:
# make TESTS=tests/integration/shared_storage_configuration/test_example_api_client.py:TestExampleApiClient.test_login ssi_tests
# set NOSE_ARGS="-x" to stop on the first failure
ssi_tests: reset_cluster
	chroma-manager/tests/framework/integration/shared_storage_configuration/full_cluster/jenkins_steps/main $@

efs_tests: reset_cluster
	pdsh -R ssh -l root -S -w vm[5-9] "echo \"options lnet networks=\\\"tcp(eth1)\\\"\" > /etc/modprobe.d/iml_lnet_module_parameters.conf; systemctl disable firewalld; systemctl stop firewalld"
	chroma-manager/tests/framework/integration/existing_filesystem_configuration/jenkins_steps/main $@

requirements:
	make -C chroma-manager requirements

chroma_test_env: requirements chroma_test_env/bin/activate

chroma_test_env/bin/activate: chroma-manager/requirements.txt
	test -d chroma_test_env || virtualenv --no-site-packages chroma_test_env
	chroma_test_env/bin/pip install -r chroma-manager/requirements.txt
	touch chroma_test_env/bin/activate

unit_tests: chroma_test_env
	sh -c '. chroma_test_env/bin/activate; make -C chroma-manager unit_tests'
