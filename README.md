Prototype write job distribution for a pool of file writers
-----------------------------------------------------------

Requires docker-compose to run.

Run with:
```
docker-compose up -d --scale consumer=10 --build
```

and bring down with
```
docker-compose down -v
```
