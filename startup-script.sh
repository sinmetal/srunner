#!/bin/bash

# Install google-fluentd
curl -sSO https://dl.google.com/cloudagents/install-logging-agent.sh
sha256sum install-logging-agent.sh
sudo bash install-logging-agent.sh
# Restart google-fluentd
service google-fluentd restart

# Your Task
echo "Start Task!"
export SRUNNER_SPANNERDATABASE="projects/your-spanner-project/instances/your-spanner-instance/databases/sinmetal"

gsutil cp gs://your-bucket/srunner.bin .
sudo chmod +x ./srunner.bin
./srunner.bin
echo "DONE Task!"
