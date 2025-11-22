# Restore da base dados

Conectar ao shell do container da base de dados
```bash
docker exec -it postgres-wine-db sh
```

Efetua login com usuário postgres
```bash
su - postgres
```

Executar o commando abaixo para efetuar o restore
```bash
pg_restore -d esd_wine < /tmp/dump/esd_wine_carregada.dump
pg_restore -d esd_wine_dw < /tmp/dump/esd_wine_dw_carregada.dump
```

Para gerar dump
```bash
pg_dump -d esd_wine -C -F c -f /tmp/dump/esd_wine_carregada.dump
pg_dump -d esd_wine_dw -C -F c -f /tmp/dump/esd_wine_dw_carregada.dump
```
