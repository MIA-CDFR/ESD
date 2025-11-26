
CREATE TABLE public.feature_utilizador (
    utilizador_id integer,
    idade integer,
    n_compras bigint,
    quantidade_total bigint,
    valor_total double precision,
    ticket_medio double precision,
    recencia_dias integer,
    n_produtos bigint,
    n_lojas bigint,
    n_regioes bigint,
    score_sentimento_medio double precision,
    n_registos_sentimento bigint,
    n_comentarios bigint
);


ALTER TABLE public.feature_utilizador OWNER TO postgres;


CREATE TABLE public.utilizador_segmento (
    utilizador_id integer NOT NULL,
    cluster integer NOT NULL,
    atualizado_em timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.utilizador_segmento OWNER TO postgres;


Insert into public.feature_utilizador (utilizador_id, idade, n_compras, quantidade_total, valor_total, ticket_medio, recencia_dias, n_produtos, n_lojas, n_regioes, score_sentimento_medio, n_registos_sentimento, n_comentarios) values
(2,	48,	470,	1656,	22061.659999999993,	46.93970212765956,	9,	88,	2,	19,	0.6444679773965124,	472,	120),
(3,	48,	500,	1798,	23519.83999999998,	47.039679999999954,	14,	97,	2,	19,	0.6457712619366522,	501,	116),
(4,	25,	552,	1928,	31651.359999999993,	57.33942028985506,	9,	96,	2,	20,	0.6445780263886961,	553,	146),
(5,	34,	478,	1730,	22626.880000000026,	47.33656903765696,	14,	88,	2,	20,	0.6458433565366741,	479,	0);




INSERT INTO public.utilizador_segmento (utilizador_id, cluster, atualizado_em) VALUES
(2,	2,	'2025-11-26 00:10:40.984822'),
(3,	0,	'2025-11-26 00:10:40.984822'),
(4,	1,	'2025-11-26 00:10:40.984822'),
(5,	3,	'2025-11-26 00:10:40.984822');




ALTER TABLE ONLY public.utilizador_segmento
    ADD CONSTRAINT utilizador_segmento_pkey PRIMARY KEY (utilizador_id);
