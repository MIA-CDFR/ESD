from pipelines.gold.load_dim_date import run_load as run_load_dim_date
from pipelines.gold.load_dim_metodo_pagamento import run_load as run_load_dim_metodo_pagamento
from pipelines.gold.load_dim_produto_categoria import run_load as run_load_dim_produto_categoria
from pipelines.gold.load_dim_produto import run_load as run_load_dim_produto
from pipelines.gold.load_dim_source import run_load as run_load_dim_source
from pipelines.gold.load_dim_tipo_sentimento import run_load as run_load_dim_tipo_sentimento
from pipelines.gold.load_dim_utilizador import run_load as run_load_dim_utilizador
from pipelines.gold.load_ft_sentimento import run_load as run_load_ft_sentimento
from pipelines.gold.load_ft_vendas import run_load as run_load_ft_vendas

run_load_dim_date()
run_load_dim_metodo_pagamento()
run_load_dim_produto_categoria()
run_load_dim_produto()
run_load_dim_source()
run_load_dim_tipo_sentimento()
run_load_dim_utilizador()
run_load_ft_sentimento()
run_load_ft_vendas()
