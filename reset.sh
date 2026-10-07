#!/bin/bash

podman compose down -v --remove-orphans
rm -f data/landing/*
rm -f orchestrator/data/orchestrator.db
