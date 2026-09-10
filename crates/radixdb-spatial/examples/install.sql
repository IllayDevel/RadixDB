CREATE EXTENSION radixdb_spatial VERSION '1.0.0';

CREATE TYPE public.point FROM EXTENSION radixdb_spatial AS 'point';
CREATE TYPE public.box2d FROM EXTENSION radixdb_spatial AS 'box2d';
CREATE TYPE public.polygon FROM EXTENSION radixdb_spatial AS 'polygon';

CREATE FUNCTION public.st_distance(left public.point NOT NULL, right public.point NOT NULL)
RETURNS FLOAT NOT NULL LANGUAGE NATIVE
FROM EXTENSION radixdb_spatial AS 'distance';

CREATE FUNCTION public.distance(left public.point NOT NULL, right public.point NOT NULL)
RETURNS FLOAT NOT NULL LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS
BEGIN
    RETURN st_distance(left, right);
END;
