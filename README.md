# dw_sandbox
Data warehouse sandbox

At this moment this is just a sandbox for creating and inspecting dataflow in bigger picture.


### Initial:
- For now just copy .env-example to .env
  - Some password are hardcoded in this stage and quite general defaults anyway


### Start:
```podman compose up --build```

### Re-run:
```podman compose up --build --force-recreate extractor loader orchestrator```

### Remove:
```podman compose down -v --remove-orphans```


### Host connections:
psql -d (see below) -h localhost -P (see below) -U admin
- Databases and ports:
  - staging: localhost:5433
  - core:    localhost:5434
  - mart:    localhost:5435
  - users:   localhost:5436


### Flatten:
- object levels: 3
- arrays: child tables
- level 4+: jsonb
