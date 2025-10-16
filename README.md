# TaikoWeb

This is reworked taiko-web version.

## Improvements

  - docker compose run once for all
  - add win desktop HTA app for running
  - add support for ./song directrory scanningg
  - add support for TJA files auto-patsing
  - add support for auto-adding songs to MongoDb at startup
  - add support for JP and EN songs

## How to negin

U need to install Docker for your system

## How to run

Make container:

```bash
docker compose build --pull --no-cache
```

Run it:

```bash
docker compose up -d
```


## Windows run app

Install WSL2
Install Docker Desktop

Git clone the repo

in repo directory run the file
```bash
start_taiko_edge.hta
```