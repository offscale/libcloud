from __future__ import with_statement

import os

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.deployment import ScriptDeployment

# Path to the private SSH key file used to authenticate
PRIVATE_SSH_KEY_PATH = os.path.expanduser('~/.ssh/id_rsa_gce')

# Path to the public SSH key file which will be installed on the server for
# the root user
PUBLIC_SSH_KEY_PATH = os.path.expanduser('~/.ssh/id_rsa_gce.pub')

with open(PUBLIC_SSH_KEY_PATH, 'r') as fp:
    PUBLIC_SSH_KEY_CONTENT = fp.read().strip()

# GCE authentication related info
SERVICE_ACCOUNT_USERNAME = '<username>@<project id>.iam.gserviceaccount.com'
SERVICE_ACCOUNT_CREDENTIALS_JSON_FILE_PATH = '/path/to/sac.json'

PROJECT_ID = 'my-gcp-project'

Driver = get_driver(Provider.GCE)
conn = Driver(SERVICE_ACCOUNT_USERNAME,
              SERVICE_ACCOUNT_CREDENTIALS_JSON_FILE_PATH,
              project=PROJECT_ID,
              datacenter='us-central1-a')

# NOTE: To view the output of this command, you need to set
# LIBCLOUD_DEBUG variable and then tail that file for the log output
step = ScriptDeployment("echo whoami ; date ; ls -la")

images = conn.list_images()
sizes = conn.list_sizes()

image = [i for i in images if i.name == 'ubuntu-1604-xenial-v20191217'][0]
size = [s for s in sizes if s.name == 'e2-micro'][0]

# NOTE: We specify which public key is installed on the instance using
# metadata functionality
ex_metadata = metadata = {
    'items': [
        {
            'key': 'ssh-keys',
            'value': 'root: %s' % (PUBLIC_SSH_KEY_CONTENT)
        }
    ]
}

# deploy_node takes the same base keyword arguments as create_node.
node = conn.deploy_node(name='libcloud-deploy-demo-1', image=image, size=size,
                        ex_metadata=metadata,
                        deploy=step, ssh_key=PRIVATE_SSH_KEY_PATH)
print(node)
