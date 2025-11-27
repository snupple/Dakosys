#!/bin/sh
cd /home/rick/test/Dakosys
cp config/config-NEW.yaml config/config.yaml
sudo docker compose run --rm dakosys run-update
cp config/config-ALL.yaml config/config.yaml
