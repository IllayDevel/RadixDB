BEGIN;
CREATE EXTENSION radixdb_pair VERSION '1.0.0';
CREATE TYPE public.pair FROM EXTENSION radixdb_pair AS 'pair';
CREATE FUNCTION public.pair_sum(value public.pair NOT NULL)
RETURNS INTEGER NOT NULL LANGUAGE NATIVE
FROM EXTENSION radixdb_pair AS 'pair_sum';
COMMIT;
